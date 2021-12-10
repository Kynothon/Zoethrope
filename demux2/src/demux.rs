use serde::{Serialize, Deserialize};
use gst::element_info;
use gst::element_error;
use gst::element_warning;
use gst::prelude::*;

#[cfg(feature = "v1_10")]
use glib::GBoxed;

use log;
use std::env;
#[cfg(feature = "v1_10")]
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use anyhow::{Error};
use derive_more::{Display, Error};

#[derive(Debug, Display, Error)]
#[display(fmt = "Missing element: {}", _0)]
struct MissingElement(#[error(not(source))] &'static str);

#[derive(Debug, Display, Error)]
#[display(fmt = "Unsupported Format: {}", _0)]
struct UnsupportedFormat(#[error(not(source))] &'static str);

#[derive(Debug, Display, Error)]
#[display(fmt = "Received error from {}: {} (debug: {:?})", src, error, debug)]
struct ErrorMessage {
    src: String,
    error: String,
    debug: Option<String>,
    source: gst::glib::Error,
}

#[derive(Serialize, Deserialize, Debug, Display, Error)]
#[serde(rename_all = "camelCase")]
#[display(fmt = "Parameters:")]
struct Rusoto {
    uri: String,
    #[serde(default = "default_region")]
    region: String,
    #[serde(default = "default_endpoint")]
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    blocksize: Option<u32>,
}

#[cfg(feature = "v1_10")]
#[derive(Clone, Debug, GBoxed)]
#[gboxed(type_name = "ErrorValue")]
struct ErrorValue(Arc<Mutex<Option<Error>>>);

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    src: Rusoto,
    sink: Rusoto,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Media {
    media_type: String,
    uri: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    medias: Vec<Media>,
}

fn default_region() -> String {
    env::var("AWS_DEFAULT_REGION").unwrap_or(String::from("us-west-2"))
}

fn default_endpoint() -> Option<String> {
    env::var("AWS_S3_ENDPOINT").ok()
}

fn to_rusoto_region_with_endpoint(rusoto: &Rusoto) -> String {
    let region = match &rusoto.endpoint {
        Some(endpoint) => {
            log::debug!("region: {}, endpoint: {}", rusoto.region, endpoint);
            let region = base32::encode(base32::Alphabet::RFC4648 { padding: false }, rusoto.region.as_bytes(),);
            let endpoint = base32::encode(base32::Alphabet::RFC4648 { padding: false }, endpoint.as_bytes(),);
            format!("{}+{}", region, endpoint)
        }
        None => rusoto.region.clone()
    };
    let path = (*rusoto).uri.strip_prefix("s3://").unwrap_or(&rusoto.uri);

    return format!("s3://{}/{}", region, path);
}

fn get_element(factory_name: &'static str) -> Result<gst::Element, MissingElement> {
    return gst::ElementFactory::make(factory_name, None)
                                .map_err(|_| MissingElement(factory_name));
}

fn demux(source_config: Rusoto, sink_config: Rusoto) -> Result<Response, Error> {
    gst::init()?;

    let mut medias = vec![];

    let pipeline = gst::Pipeline::new(None);
    let src = gst::ElementFactory::make("rusotos3src", None).map_err(|_| MissingElement("rusotos3src"))?;
    let decodebin =
        gst::ElementFactory::make("parsebin", None).map_err(|_| MissingElement("decodebin"))?;

    let uri = to_rusoto_region_with_endpoint(&source_config);
    log::debug!("SRC URI: {}", uri);
    // Tell the filesrc what file to load
    src.set_property("uri", &(uri.as_str()))?;
    if let Some(access_key) = source_config.access_key_id {
        log::debug!("SRC Access Key ID: {}", access_key);
        src.set_property("access-key", &(access_key.as_str()))?;
    }
    if let Some(secret_access_key) = source_config.secret_access_key {
        log::debug!("SRC Secret Access Key: {}", secret_access_key);
        src.set_property("secret-access-key", &(secret_access_key.as_str()))?;
    }
    if let Some(blocksize) = source_config.blocksize {
        log::debug!("SRC Blocksize: {}", blocksize);
        src.set_property("blocksize", blocksize)?;
    }

   
    pipeline.add_many(&[&src, &decodebin])?;
    gst::Element::link_many(&[&src, &decodebin])?;

    // Need to move a new reference into the closure.
    // !!ATTENTION!!:
    // It might seem appealing to use pipeline.clone() here, because that greatly
    // simplifies the code within the callback. What this actually does, however, is creating
    // a memory leak. The clone of a pipeline is a new strong reference on the pipeline.
    // Storing this strong reference of the pipeline within the callback (we are moving it in!),
    // which is in turn stored in another strong reference on the pipeline is creating a
    // reference cycle.
    // DO NOT USE pipeline.clone() TO USE THE PIPELINE WITHIN A CALLBACK
    let pipeline_weak = pipeline.downgrade();
    // Connect to decodebin's pad-added signal, that is emitted whenever
    // it found another stream from the input file and found a way to decode it to its raw format.
    // decodebin automatically adds a src-pad for this raw stream, which
    // we can use to build the follow-up pipeline.
    decodebin.connect_pad_added(move |dbin, src_pad| {
        // Here we temporarily retrieve a strong reference on the pipeline from the weak one
        // we moved into this callback.
        let pipeline = match pipeline_weak.upgrade() {
            Some(pipeline) => pipeline,
            None => return,
        };


        // Try to detect whether the raw stream decodebin provided us with
        // just now is either audio or video (or none of both, e.g. subtitles).
        let media_type = {
            let media_type = src_pad.current_caps().and_then(|caps| {
                caps.structure(0).map(|s| {
                    s.name()
                })
            });

            match media_type {
                None => {
                    element_warning!(
                        dbin,
                        gst::CoreError::Negotiation,
                        ("Failed to get media type from pad {}", src_pad.name())
                    );

                    return;
                }
                Some(media_type) => media_type,
            }
        };

        // We create a closure here, calling it directly below it, because this greatly
        // improves readability for error-handling. Like this, we can simply use the
        // ?-operator within the closure, and handle the actual error down below where
        // we call the insert_sink(..) closure.
        let insert_sink =  |media_type | -> Result<(), Error> {
                // decodebin found a raw videostream, so we build the follow-up pipeline to
                // display it using the autovideosink.
                let queue = get_element("queue")?;
               
                let parse = match media_type {
                    "audio/mpeg" => get_element("aacparse")?,
                    "video/x-h264" => get_element("h264parse")?,
                    "video/x-h265" => get_element("h265parse")?, 
                    "video/x-av1" => get_element("av1parse")?,
                    content_type => Err(UnsupportedFormat(content_type))?,
                };

                let mux = get_element("qtmux")?;
                let sink = get_element("rusotos3sink")?;

                let uri_pattern = to_rusoto_region_with_endpoint(&sink_config);
                let uuid = Uuid::new_v4();
                let uri = str::replace(&uri_pattern, "%u", &uuid.to_string());
                log::debug!("SINK URI: {}", uri);
              
                mux.set_property("faststart", &true)?;

                sink.set_property("async", &true)?;
                sink.set_property("sync", &false)?;
                sink.set_property("uri", &uri)?;

                let content = gst::Structure::builder("new-object")
                    .field("uri", str::replace(&sink_config.uri, "%u", &uuid.to_string()))
                    .field("content-type", media_type)
                    .build();
                pipeline.post_message(gst::message::Application::new(content))?;


                let elements = &[&queue, &parse, &mux, &sink];
                pipeline.add_many(elements)?;
                gst::Element::link_many(elements)?;

                for e in elements {
                    e.sync_state_with_parent()?
                }

                // Get the queue element's sink pad and link the decodebin's newly created
                // src pad for the video stream to it.
                let sink_pad = queue.static_pad("sink").expect("queue has no sinkpad");
                src_pad.link(&sink_pad)?;

                element_info!(
                    dbin,
                    gst::LibraryError::Failed,
                    ("Success to insert sink")
            );


                Ok(())
        };

        // When adding and linking new elements in a callback fails, error information is often sparse.
        // GStreamer's built-in debugging can be hard to link back to the exact position within the code
        // that failed. Since callbacks are called from random threads within the pipeline, it can get hard
        // to get good error information. The macros used in the following can solve that. With the use
        // of those, one can send arbitrary rust types (using the pipeline's bus) into the mainloop.
        // What we send here is unpacked down below, in the iteration-code over sent bus-messages.
        // Because we are using the failure crate for error details here, we even get a backtrace for
        // where the error was constructed. (If RUST_BACKTRACE=1 is set)
        //
        match insert_sink(media_type) {
            Err(err) => {
            // The following sends a message of type Error on the bus, containing our detailed
            // error information.
            #[cfg(feature = "v1_18")]
            element_error!(
                dbin,
                gst::LibraryError::Failed,
                ("Failed to insert sink"),
                details: gst::Structure::builder("error-details")
                            .field("error",
                                   &ErrorValue(Arc::new(Mutex::new(Some(err)))))
                            .build()
            );

            #[cfg(not(feature = "v1_18"))]
            element_error!(
                dbin,
                gst::LibraryError::Failed,
                ("Failed to insert sink"),
                ["{}", err]
            );
            }
            Ok(_) => ()
        }
    });

    pipeline.set_state(gst::State::Playing)?;

    let bus = pipeline
        .bus()
        .expect("Pipeline without bus. Shouldn't happen!");

    // This code iterates over all messages that are sent across our pipeline's bus.
    // In the callback ("pad-added" on the decodebin), we sent better error information
    // using a bus message. This is the position where we get those messages and log
    // the contained information.
    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        use gst::MessageView;

        match msg.view() {
            MessageView::Eos(..) => break,
            MessageView::Error(err) => {
                pipeline.set_state(gst::State::Null)?;

                #[cfg(feature = "v1_10")]
                {
                    match err.get_details() {
                        // This bus-message of type error contained our custom error-details struct
                        // that we sent in the pad-added callback above. So we unpack it and log
                        // the detailed error information here. details contains a glib::SendValue.
                        // The unpacked error is the converted to a Result::Err, stopping the
                        // application's execution.
                        Some(details) if details.get_name() == "error-details" => details
                            .get::<&ErrorValue>("error")
                            .unwrap()
                            .and_then(|v| v.0.lock().unwrap().take())
                            .map(Result::Err)
                            .expect("error-details message without actual error"),
                        _ => Err(ErrorMessage {
                            src: msg
                                .get_src()
                                .map(|s| rwring::from(s.get_path_string()))
                                .unwrap_or_else(|| String::from("None")),
                            error: err.get_error().to_string(),
                            debug: err.get_debug(),
                            source: err.get_error(),
                        }
                        .into()),
                    }?;
                }
                #[cfg(not(feature = "v1_10"))]
                {
                    return Err(ErrorMessage {
                        src: msg
                            .src()
                            .map(|s| String::from(s.path_string()))
                            .unwrap_or_else(|| String::from("None")),
                        error: err.error().to_string(),
                        debug: err.debug(),
                        source: err.error(),
                    }
                    .into());
                }
            }
            MessageView::StateChanged(s) => {
                log::debug!(
                    "State changed from {:?}: {:?} -> {:?} ({:?})",
                    s.src().map(|s| s.path_string()),
                    s.old(),
                    s.current(),
                    s.pending()
                );
            }
            gst::MessageView::Application(application) => {
                let s = application.structure().unwrap();

                match s.name() {
                    "new-object" => {
                        let uri = s.get::<String>("uri").unwrap();
                        let content_type = s.get::<String>("content-type").unwrap();

                        medias.push(Media{uri: uri, media_type: content_type});
                    },
                    _ => ()
                }

            }
            _ => ()
        }
    }

    pipeline.set_state(gst::State::Null)?;
    Ok(Response{medias: medias})
}

pub fn demux_main(request: Request) -> Result<Response, Error> {

    return demux(request.src, request.sink);
}
