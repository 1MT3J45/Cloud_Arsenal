import json
import boto3


def lambda_handler(event, context):
    response_items = s3_put("bucket_name", "file_name/", "payload")
    return response_items


def s3_put(bucket_name, file_path, payload):
    s3c = boto3.client('s3')

    response = s3c.Object(bucket_name, file_path+"name.extension").put(Body=payload)
    response_items = response
    return response_items


def s3_download(bucket_name, file_path):
    from urllib.parse import unquote_plus, unquote
    from os import listdir

    s3r = boto3.resource('s3')
    file_path = unquote_plus(unquote(file_path))
    file_extension = file_path.split(".")[-1]
    try:
        s3r.Object(bucket_name, file_path).download_file(f"/tmp/file.{file_extension}")
        print(listdir("/tmp/"))
        return True
    except Exception as e:
        print(f"[ERR] {type(e)} MORE: {str(e)}")
        print(listdir("/tmp/"))
        return False


def s3_upload(bucket_name, s3_path, file_to_upload):
    s3c = boto3.client('s3')

    try:
        s3c.meta.client.upload_file(file_to_upload, bucket_name, s3_path)
        return True
    except Exception as e:
        print(f"[ERR] File upload failed! TYPE: {type(e)} MORE: {str(e)}")
        return False


def s3_list_objects(bucket_name, prefix=False):
    s3c = boto3.client('s3')
    response = []

    try:
        if prefix:
            response = s3c.list_objects(Bucket=bucket_name, Prefix=prefix)
        else:
            response = s3c.list_objects(Bucket=bucket_name)
        return response
    except Exception as e:
        print(f"[ERR] Something went wrong there. More:{type(e)} : {str(e)}")
        return response
