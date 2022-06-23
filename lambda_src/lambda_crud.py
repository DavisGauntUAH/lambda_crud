from email import message
import logging
import os
from botocore.exceptions import ClientError
import boto3

LOCALSTACK_INTERNAL_ENDPOINT_URL = f'http://{os.environ.get("LOCALSTACK_HOSTNAME")}:4566'

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

def create_bucket(bucket_name, s3):
    """
    Creates an s3 bucket.  Takes in a bucket name and the
    neccisary s3 object.
    """
    try:
        resp = s3.create_bucket(Bucket=bucket_name)
    except ClientError as err:
        logger.exception(f'Unable to create  S3 bucket locally. Error : {err}')
    else: return resp


def empty_bucket(b_name, s3_resource):
    """
    Deletes all the objects in an s3 bucket.  Takes in a
    bucket name and the neccisary s3 resource
    """
    try:
        bucket = s3_resource.Bucket(b_name)
        resp = bucket.objects.all().delete()
    except Exception as err:
        logger.exception(f'Error : {err}')
        raise
    else:
        return resp
    
    
def del_bucket(b_name, s3_r, s3_c):
    """
    Deletes and enpties a buckey.  Takes in a bucket name and the 
    neccisary s3 objects
    """
    try:
        empty_bucket(b_name, s3_r)
        resp = s3_c.delete_bucket(Bucket=b_name)
    except Exception as err:
        logger.exception(f'Error: could not delete bucket: {err}')
    else:
        return resp
    
    
def del_file(b_name, f_name, s3):
    """
    Deletes a file from an s3 bucket.  Takes in a bucket name,
    a file name/key and the neccisary s3 object
    """
    try:
        s3.Object(b_name, f_name).delete()
    except Exception as err:
        logger.exception(f'Error: could not delete File: {err}')
    
    
def list_bucket_contents(b_name, s3):
    """
    Lists the contents of an s3 bucket. Takes in a bucket name
    and the neccisary s3 object
    """
    try:
        bucket = s3.Bucket(b_name)
        resp = []
        for obj in bucket.objects.all():
            resp.append(obj.key)
    except Exception as err:
        logger.exception(f'Error: {err}')
    else:
        return resp
    
    
def read_file(bucket, key, s3):
    """
    reads the contents of an s3 object and returns the contents.
    Takes in a bucket name, key, and required s3 object
    """
    try:
        bucket = s3.Bucket(bucket)
        obj = bucket.Object(key)
        resp = obj.get()['Body'].read().decode('utf-8')
    except Exception as err:
      logger.exception(f'Error: {err}')
    else:
        return resp
    
    
def write_obj(bucket, key, data, s3):
    """
    Writes out to or over writes an object in a s3 bucket.
    Takes in a bucket name, key, the data to write, and 
    required s3 object
    """
    try:
      s3.put_object( Bucket=bucket, Key=key, Body=data)
    except Exception as err:
      logger.exception(f'Error: {err}') 
      
      
def append_obj(bucket, key, data, s3, s3_c):
    """
    appends data to an object stored in an s3 bucket.
    Takes in a bucket name, key, data to append, and
    required s3 objects
    """
    old_data = read_file(bucket, key, s3)
    new_data = old_data+'\n'+data
    write_obj(bucket, key, new_data, s3_c)
    
    
def get_boto3_client(service, region):
    """
    creates a boto service client.  Takes in the service type
    and aws region
    """
    try:
      client = boto3.client(service,
                            region_name=region,
                            endpoint_url=LOCALSTACK_INTERNAL_ENDPOINT_URL)
    except Exception as err:
      logger.exception(f'Error while connecting to localstack: Error: {err}')
    else:
        return client
    
    
def get_boto3_resource(service, region):
    """
    creates a boto service resource.  Takes in the service type
    and aws region
    """
    try:
        resource = boto3.resource(service,
                            region_name=region,
                            endpoint_url=LOCALSTACK_INTERNAL_ENDPOINT_URL)
    except Exception as err:
        logger.exception(f'Error while connecting to localstack: Error: {err}')
    else:
        return resource


def handler(event, context):
    
    region = event['aws_region']
    action = event['task']
    ret = ''
    
    s3_client = get_boto3_client("s3", region)
    s3_resource = get_boto3_resource("s3", region)
    
    bucket = event[action]['bucket_name']
    
    if(action == 'delete_bucket'):
        del_bucket(bucket, s3_resource, s3_client)
    elif(action == 'delete_object'):
        key = event[action]['key']
        del_file(bucket, key, s3_resource) 
    elif(action == 'read_object'):
        key = event[action]['key']
        ret = read_file(bucket, key, s3_resource)
    elif(action == 'get_objects'):
        obj_list = list_bucket_contents(bucket, s3_resource)
        for obj in obj_list:
            ret = ret + f's3://{bucket}/{obj}\n'
    elif(action == 'write_object'):
        key = event[action]['key']
        data = event[action]['write_data']
        write_obj(bucket, key, data, s3_client)
    elif(action == 'append_object'):
        key = event[action]['key']
        data = event[action]['write_data']
        append_obj(bucket, key, data, s3_resource, s3_client)
    elif(action == 'make_bucket'):
        create_bucket(bucket, s3_client)
    else:
        logger.error(f'{action} is an invalid task.')
        
    return {"message": ret}