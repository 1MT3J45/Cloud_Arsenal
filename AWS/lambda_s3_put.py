import json
import boto3


def lambda_handler(event, context):
    response_items = s3_put("bucket_name", "file_name/", "payload")
    return response_items


def s3_put(bucket_name, file_path, file_name, payload):
    """
    Use for Uploading a file as StringIO object
    :param str bucket_name: s3 bucket name
    :param str file_path: string name, sub folders are allowed
    :param str file_name: name that you need to be in destination S3 Bucket location
    :param StringIO payload: CSV to be uploaded as stringIO Object
    :return:
    """
    s3r = boto3.resource('s3')
    response = s3r.Object(bucket_name, file_name).put(Body=payload.getvalue())

    s3c = boto3.client('s3')

    response = s3c.put_object(Bucket=bucket_name, Key=file_path + file_name, Body=payload)
    status_code = response.get("ResponseMetadata").get("HTTPStatusCode")
    if status_code == 200:
        return True, status_code
    else:
        return False, status_code

def s3_download(bucket_name, file_path, file_name):
    """
    Download a file from an S3 Bucket

    :param str bucket_name: Pass only the bucket name without any "s3://" or "s3a://", just the name
    :param str file_path: Path where your file. Make sure to include the name of your file not just the folder path
    :param str file_name: Name of the file you need it to be, with it's extension ex. my_file.txt
    :return bool:
    """
    from urllib.parse import unquote_plus, unquote
    from os import listdir

    s3c = boto3.client('s3')
    file_path = unquote_plus(unquote(file_path))
    file_extension = file_path.split(".")[-1]
    try:
        s3c.download_file(bucket_name, file_path, file_name)
        print("[mLOG] Files in /tmp/ \n", listdir("/tmp/"))
        return True
    except Exception as e:
        print(f"[ERR] {type(e)} MORE: {str(e)}")
        print("[mLOG] Files in /tmp/ \n", listdir("/tmp/"))
        return False


def s3_upload(bucket_name, s3_path, file_to_upload):
    """

    :param str bucket_name: Pass only the bucket name without any "s3://" or "s3a://", just the name
    :param str s3_path: Path of your s3 folder where you want your file dumped
    :param str file_to_upload: local file path
    :return:
    """
    s3c = boto3.client('s3')

    try:
        s3c.upload_file(file_to_upload, bucket_name, s3_path)
        return True
    except Exception as e:
        print(f"[ERR] File upload failed! TYPE: {type(e)} MORE: {str(e)}")
        return False


def s3_list_objects(bucket_name, prefix=False):
    s3c = boto3.client('s3')
    response = []

    try:
        if prefix:
            response = s3c.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        else:
            response = s3c.list_objects_v2(Bucket=bucket_name)
        return response
    except Exception as e:
        print(f"[ERR] Something went wrong there. More:{type(e)} : {str(e)}")
        return response
