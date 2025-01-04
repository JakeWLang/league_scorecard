from os import environ
import json
from r2connect.r2client import R2Client
from r2connect import exceptions

secret_names = ['ENDPOINT_URL', 'ACCESS_KEY', 'SECRET_KEY', 'REGION']
with open('secrets.json', 'r') as f:
    secrets = json.loads(f.read())
for secret in secret_names:
    environ[secret] = secrets[secret]
BUCKET_NAME = secrets['bucket_name']


def init_client():
    try:
        r2_client = R2Client()
    except exceptions.cloudflare.r2.MissingConfig as error:
        print(error)
    return r2_client


def delete_file(r2_client, object_name, bucket_name):
    try:
	    r2_client.delete_file(object_name, bucket_name)
    except exceptions.cloudflare.r2.ObjectDoesNotExist as error:
        print(f"The specified object does not exist in this bucket: {object_name}")
    except exceptions.cloudflare.r2.BucketDoesNotExist as error:
        print(f"The specified bucket does not exist: {bucket_name}")
    except Exception as error:
        print(error)


def upload_file(r2_client, file_path, object_name, reupload):
    try:
        r2_client.upload_file(file_path, object_name, BUCKET_NAME)
        print(f'Successfully uploaded {object_name} to {BUCKET_NAME}')
    except exceptions.cloudflare.r2.BucketDoesNotExist as error:
        print(f"The specified bucket does not exist: {BUCKET_NAME}")
    except exceptions.cloudflare.r2.ObjectAlreadyExists as error:
        print(f"An object with the same object_key already exists: {object_name}")
        if reupload:
            print(f'killing current instance of {object_name} and reuploading')
            delete_file(r2_client, object_name, BUCKET_NAME)
            upload_file(r2_client, file_path, object_name, False)
    except Exception as error:
        print(error)


def upload_new(r2_client, is_new, file_path, object_name):
    if is_new:
        upload_file(r2_client, file_path, object_name, False)
    return
