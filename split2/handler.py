#! /usr/bin/env python3
import logging

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib, GObject

import boto3
from botocore.exceptions import ClientError

from urllib.parse import urlparse
import concurrent.futures
import os
import json
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

def split(source_config, sink_config, config):
    pipeline = Gst.Pipeline.new('split')

    def insert_sink(src_pad, media_type):
        queue = Gst.ElementFactory.make('queue')

        parse = None
        if media_type == 'video/x-h264':
            parse = Gst.ElementFactory.make('h264parse')
        elif media_type == 'video/x-h265':
            parse = Gst.ElementFactory.make('h265parse')
        elif media_type == 'video/x-av1':
            parse = Gst.ElementFactory.make('av1parse')

        sink = Gst.ElementFactory.make('splitmuxsink')

        max_size_time = config.get('maxSizeTime', None)
        max_size_bytes = config.get('maxSizeBytes', None)
        
        if max_size_time is not None:
            sink.set_property('max-size-time', max_size_time)
        elif max_size_bytes is not None:
            sink.set_property('max-size-bytes', max_size_bytes)
        
        tempdir = tempfile.mkdtemp()
        logging.debug(f'{tempdir}/{sink_config["uri"].rsplit("/", 1)[1]}')

        sink.set_property('location', f'{tempdir}/{sink_config["uri"].rsplit("/", 1)[1]}')
   
        pipeline.add(queue)
        pipeline.add(parse)
        pipeline.add(sink)
        
        queue.link(parse)
        parse.link(sink)

        queue.sync_state_with_parent()
        parse.sync_state_with_parent()
        sink.sync_state_with_parent()

        sink_pad = queue.get_static_pad('sink')
        src_pad.link(sink_pad)

    def on_parsebin_pad_added(parsebin, srcpad, *user_data):
        media_type = srcpad.get_current_caps()
        insert_sink(srcpad, media_type.get_structure(0).get_name())

    src = Gst.ElementFactory.make('rusotos3src')
   
    uri = to_rusoto_region_with_endpoint(source_config)
    logging.debug(f'SRC URI: {uri}')
    src.set_property('uri', uri)

    access_key = os.environ.get('AWS_ACCESS_KEY', None)
    if access_key is not None:
        logging.debug(f'SRC Access Key ID: {access_key}')
        src.set_property('access-key', access_key)
 
    secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
    if access_key is not None:
        logging.debug(f'SRC Secret Access Key: {access_key}')
        src.set_property('secret-access-key', secret_access_key)

    blocksize = source_config.get('blocksize', None)
    if blocksize is not None:
        logging.debug(f'SRC Blocksize: {blocksize}')
        src.set_property('blocksize', blocksize)

    parsebin = Gst.ElementFactory.make('parsebin')

    pipeline.add(src)
    pipeline.add(parsebin)

    src.link(parsebin)

    parsebin.connect("pad-added", on_parsebin_pad_added)

    return pipeline

def executor_bus_call(max_workers, boto, output):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    uri = urlparse(boto['uri'])
    endpoint = boto.get('endpoint', os.environ.get('AWS_S3_ENDPOINT', None)) 
    region = boto.get('region', os.environ.get('AWS_DEFAULT_REGION', 'us-west-2'))
    s3 = boto3.resource('s3', endpoint_url=endpoint)
    bucket = uri.hostname
    prefix = uri.path.rsplit('/', 1)[0][1:]


    def bus_call(bus, message, loop):     
        t = message.type     
        if t == Gst.MessageType.EOS:         
            print("End-of-stream\n")         
            loop.quit()     
        elif t == Gst.MessageType.ERROR: 
            err, debug = message.parse_error()         
            print("Error: %s: %s\n" % (err, debug))         
            loop.quit()     
            return True
        elif t == Gst.MessageType.ELEMENT:
            structure=message.get_structure()
            if message.src.get_name().startswith('splitmuxsink'):
                if structure.get_name()=='splitmuxsink-fragment-closed':
                    filepath = structure.get_string("location")
                    filename = filepath.rsplit("/", 1)[1]
                    executor.submit(s3.Bucket(bucket).upload_file(filepath, f'{prefix}/{filename}', ExtraArgs={'ContentType': 'application/octet-stream'}))
                    output.append({"uri": f"s3://{bucket}/{prefix}/{filename}"}) 
                    os.remove(filepath) 

            return True

    return bus_call

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    Gst.init(None)
    
    body = json.loads(req)

    pipeline = split(body['src'], body['sink'], body['params'])
    output = []

    bus = pipeline.get_bus()
    bus.add_signal_watch()
    gstLoop = GLib.MainLoop()
    bus.connect ("message", executor_bus_call(5, body['sink'], output), gstLoop)


    pipeline.set_state(Gst.State.PLAYING)
    try:
        gstLoop.run()
    except:
        pass         
    # cleanup   
    pipeline.set_state(Gst.State.NULL) 

    return json.dumps({'medias': output}), 200, {'Content-Type': 'application/json'}


if __name__ == "__main__":
    req = """
    Some data
    """
    
    res = handle(req)
    print(res)

