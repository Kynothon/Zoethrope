#! /usr/bin/env python3
# I know it's ugly but it's intented to be used as executable 
# but i want to use it as library
mp4_dash = __import__('mp4-dash')
import boto3
from botocore.exceptions import ClientError

import os
import concurrent.futures

import subprocess

import json

from urllib.parse import urlparse
import tempfile
import uuid

def pull_files(source_config, temp_dir, max_workers = 5):
    s3 = boto3.resource('s3',
                  endpoint_url=os.environ.get('AWS_S3_ENDPOINT', None))
    local_files = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for media in source_config['medias']:
            uri = urlparse(media['uri'])
            bucket = uri.hostname
            file = media["uri"][4:]
            tmpfile = uuid.uuid4()
            executor.submit(s3.Bucket(bucket).download_file(f'{uri.path}',f'{temp_dir}/{tmpfile}'))
            local_files.append(f'{temp_dir}/{tmpfile}')

    return local_files

def push_files(sink_config, files, max_workers = 5):
    s3 = boto3.resource('s3',
                  endpoint_url=os.environ.get('AWS_S3_ENDPOINT', None))
    remote_files = []
    uri = urlparse(sink_config['uri'])
    bucket = uri.hostname

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for file in files:
            filename = file.split('/')[-1]
            executor.submit(s3.Bucket(bucket).upload_file(file, f'{uri.path}/{filename}'))
            remote_files.append({'uri': f's3://{bucket}{uri.path}{filename}'})

    return remote_files




def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    body = json.loads(req)
    with tempfile.TemporaryDirectory() as tempdirname:
        local_files = pull_files(body['src'], tempdirname)

    # it's ugly but later will use the library maybe
        cmd = ["python3", "/opt/bento4/utils/mp4-dash.py", "-f", "-o", f"{tempdirname}/output", "--no-split", "--hls", "--profile=on-demand"]
        cmd.extend(local_files)
        subprocess.run(cmd)

        files_to_upload = [f'{tempdirname}/output/{filename}' for filename in os.listdir(f'{tempdirname}/output')]

        uploaded = push_files(body['sink'], files_to_upload)

    return json.dumps({'medias': uploaded}), 200, {'Content-Type': 'application/json'}
    
if __name__ == "__main__":
    req = """
    {
        "uris": [
            "s3://us-east-1/output/out/4c554137-0eac-457f-b977-05795d2204f2.mp4",
            "s3://us-east-1/output/out/caa77c4a-38e9-4c7c-8b09-5c6d78405919.mp4"
        ],
        "bucket": "output",
        "prefix": "cmaf"
    }
    """
    
    res = handle(req)
    print(res)

