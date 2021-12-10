extern crate serde_json;
extern crate mime;

use hyper::{header, Body, Request, Response, StatusCode};
use hyper::body::Buf;

mod demux;

pub async fn handle(req: Request<Body>) -> Response<Body> {
   
    let payload = hyper::body::aggregate(req).await;
    let body = payload.unwrap();

    let demux_request: demux::Request = serde_json::from_reader(body.reader()).unwrap();
    
    let result : demux::Response = demux::demux_main(demux_request).unwrap();

    let payload = serde_json::to_string(&result).unwrap();

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())
        .body(Body::from(payload));
    response.ok().unwrap()
}
