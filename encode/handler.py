#! /usr/bin/env python3
import logging

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib, GObject

import boto3
from botocore.exceptions import ClientError

import concurrent.futures
import os
import json
import time
import base64
import tempfile

def to_rusoto_region_with_endpoint(rusoto):
    endpoint = rusoto.get('endpoint', os.environ.get('AWS_S3_ENDPOINT', None)) 
    region = rusoto.get('region', os.environ.get('AWS_DEFAULT_REGION', 'us-west-2'))
    if endpoint is not None:
        logging.debug(f'region: {region}, endpoint: {endpoint}')
        b32_region = base64.b32encode(bytes(region, 'utf-8')).decode('utf-8')
        b32_endpoint = base64.b32encode(bytes(endpoint, 'utf-8')).decode('utf-8')
        region = f'{b32_region}+{b32_endpoint}'
   
    path = rusoto['uri'].removeprefix('s3://')
    return f's3://{region}/{path}'



def encode(source_config, sink_config, config):

    #pipeline = Gst.parse_launch("""
    #                            concat name=c ! queue ! parsebin !  qtmux ! filesink location=both 
    #                            """)

    pipeline = Gst.Pipeline.new('encode')

    def on_demux_pad_added(demux, src_pad, *user_data):
        sink_pad = user_data[0].get_static_pad('sink')
        res = src_pad.link(sink_pad)


    def on_parsebin_pad_added(parsebin, src_pad, *user_data):
        #if media_type.startswith('video'):
        #    sink_pad_template = user_data[0].get_pad_template('video_%u')
        #else:
        #    sink_pad_template = user_data[0].get_pad_template('audio_%u')
        #sink_pad_template = user_data[0].get_pad_template('video_%u')
        #sink_pad = user_data[0].request_pad(sink_pad_template, None, None)
        sink_pad = user_data[0].get_static_pad('sink')
        res = src_pad.link(sink_pad)

    src = Gst.ElementFactory.make('rusotos3src')

    uri = to_rusoto_region_with_endpoint(source_config['media'])
    logging.debug(f'SRC URI: {uri}')
    src.set_property('uri', uri)

    blocksize = source_config.get('blocksize', None)
    if blocksize is not None:
        logging.debug(f'SRC Blocksize: {blocksize}')
        src.set_property('blocksize', blocksize)

    demux = Gst.ElementFactory.make('qtdemux')

    typefind = Gst.ElementFactory.make('typefind')

    pipeline.add(src)
    pipeline.add(demux)
    pipeline.add(typefind)

    src.link(demux)

    demux.connect('pad-added', on_demux_pad_added, typefind, pipeline)


    queue = Gst.ElementFactory.make('queue')
    decodebin = Gst.ElementFactory.make('decodebin')
    encode = Gst.ElementFactory.make('x264enc')

    encode_properties = [ prop.name for prop in encode.list_properties() ]
    for key in config.keys():
        if key in encode_properties:
            encode.set_property(key, config[key])


    qtmux = Gst.ElementFactory.make('qtmux')
    
    fragment_duration = config.get('fragmentDuration', None)
    if fragment_duration is not None:
        logging.debug(f'Sink Fragment Duration: {fragment_duration}')
        qtmux.set_property("faststart", True)
        qtmux.set_property("fragment-duration", fragment_duration)

    sink = Gst.ElementFactory.make('rusotos3sink')

    uri = to_rusoto_region_with_endpoint(sink_config)
    logging.debug(f'SINK URI: {uri}')
    sink.set_property('uri', uri)
    sink.set_property('async', True)
    sink.set_property('sync', False)

    pipeline.add(queue)
    pipeline.add(decodebin)
    pipeline.add(encode)
    pipeline.add(qtmux)
    pipeline.add(sink)

    typefind.link(queue)
    queue.link(decodebin)
    encode.link(qtmux)
    qtmux.link(sink)

    decodebin.connect('pad-added', on_parsebin_pad_added, encode, pipeline)
    
    return pipeline

def bus_call(bus, message, loop):     
    t = message.type     
    if t == Gst.MessageType.EOS:         
        loop.quit()     
    elif t == Gst.MessageType.ERROR: 
        err, debug = message.parse_error()         
        loop.quit()     
        return True

    return bus_call

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    Gst.init(None)
    print(f'req: {req}')
    
    body = json.loads(req)

    pipeline = encode(body['src'], body['sink'], body['params'])

    bus = pipeline.get_bus()
    bus.add_signal_watch()
    gstLoop = GLib.MainLoop()
    bus.connect ("message", bus_call, gstLoop)

    pipeline.set_state(Gst.State.PLAYING)
    try:
        gstLoop.run()
    except:
        pass         
    # cleanup   
    pipeline.set_state(Gst.State.NULL) 

    return json.dumps({'media': {'uri': f'{body["sink"]["uri"]}'}}), 200, {'Content-Type': 'application/json'}

if __name__ == "__main__":
    req = """
    {"source": "s3://us-east-1/output/out/video_0004.mov", "bucket": "output", "prefix": "out", "object": "blip.mov"}
    """
    
    res = handle(req)
    #res = asyncio.run(handle(req))
    print(res)

