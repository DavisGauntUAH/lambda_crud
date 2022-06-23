import glob
import json
import logging
from aiohttp import Payload
import boto3
from botocore.exceptions import ClientError
import os

AWS_REGION = os.environ.get('AWS_REGION')
AWS_PROFILE = os.environ.get('AWS_PROFILE')
ENDPOINT_URL = os.environ.get('LOCALSTACK_ENDPOINT_URL')

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

boto3.setup_default_session(profile_name=AWS_PROFILE)
s3_client = boto3.client("s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)
s3_resource = boto3.resource("s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)


def create_bucket(bucket_name):
    """
    creates a bucket in s3.  Takes in a bucket name.
    """
    payload = {
        'task' : 'make_bucket',
        'aws_region' : AWS_REGION,
        'make_bucket' : {
            'bucket_name' : bucket_name,
        }
    }
    call_lambda(payload)
    
    
def del_bucket(b_name):
    """
    Deletes a bucket from s3,  Takes in a bucket name
    """
    payload = {
        'task' : 'delete_bucket',
        'aws_region' : AWS_REGION,
        'delete_bucket' : {
            'bucket_name' : b_name,
        }
    }
    call_lambda(payload)
    
    
def del_file(b_name, f_name):
    """
    Deletes a file from s3.  Takes in a bucket name and a file name/key
    """
    payload = {
        'task' : 'delete_object',
        'aws_region' : AWS_REGION,
        'delete_object' : {
            'bucket_name' : b_name,
            'key' : f_name
        }
    }
    call_lambda(payload)        
    
    
def upload_file(f_path, bucket, obj_name=None):
    """
    uploads a file to s3.  Takes in a file path, bucket name and optionaly a name for
    the object in s3
    """
    if obj_name is None: obj_name = os.path.basename(f_path)
    data = ''
    with open(f_path, 'r') as in_file:
        data = in_file.read()
    payload = {
        'task' : 'write_object',
        'aws_region' : AWS_REGION,
        'write_object' : {
            'bucket_name' : bucket,
            'key' : obj_name,
            'write_data' : data
        }
    }
    call_lambda(payload)
    
    
def list_bucket_contents(b_name):
    """
    lists the contents of a bucket taking in a bucket name.
    uses lambda function to preform the action  retruns the contents
    of the bucket
    """
    payload = {
        'task' : 'get_objects',
        'aws_region' : AWS_REGION,
        'get_objects' : {
            'bucket_name' : b_name,
        }
    }
    resp = call_lambda(payload)
    return resp


def append_object(b_name, key, data):
    """
    appends data to the end of an object taking in a bucket name, key, and
    data to append.  uses lambda function to preform the action
    """
    payload = {
        'task' : 'append_object',
        'aws_region' : AWS_REGION,
        'append_object' : {
            'bucket_name' : b_name,
            'key' : key,
            'write_data' : data
        }
    }
    call_lambda(payload)
    
    
def read_object(b_name, key):
    """
    reads in an object from a bucket taking in a bucket name and key
    uses lambda function to preform the action  retruns the contents
    of the object
    """
    payload = {
        'task' : 'read_object',
        'aws_region' : AWS_REGION,
        'read_object' : {
            'bucket_name' : b_name,
            'key' : key
        }
    }
    return call_lambda(payload)


def call_lambda(payload):
    """
    takes in a payload and invokes lambda_crud with that payload,
    and handles the return for use
    """    
    lmb_crud = boto3.client('lambda', region_name=AWS_REGION, 
                            endpoint_url=ENDPOINT_URL)
    payload = json.dumps(payload, indent=3)
    objects = lmb_crud.invoke(FunctionName="lambda_crud",
                    InvocationType='RequestResponse',
                    Payload=payload)

    data = objects['Payload'].read()
    return json.loads(data.decode('utf-8')) 
     


def main():

    b_name = 'davis-crud-bucket'
    
    logger.info('Creating S3 bucket on localy ...')
    create_bucket(b_name)
    
    files = glob.glob('./import/*.txt')
    for file in files:
        upload_file(file, b_name)
        
    s3_log = list_bucket_contents(b_name)
    print(s3_log['message'])
    
    del_file(b_name, 'file1.txt')
    s3_log = list_bucket_contents(b_name)
    print(s3_log['message'])    
    
    msg = "Message added by lambda!!!"
    append_object(b_name, 'file2.txt', msg)
    s3_log = read_object(b_name, 'file2.txt')
    print(s3_log['message'])
    
#    time.sleep(2)
    del_bucket(b_name)

if __name__ == '__main__':
    main()