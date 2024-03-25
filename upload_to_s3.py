import os
import boto3
import json
import logging
from botocore.exceptions import ClientError

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region
    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).
    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client("s3")
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client("s3", region_name=region)
            location = {"LocationConstraint": region}
            s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=location
            )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_logData():
    # where the log data resides in the instance
    dire = r"/home/ubuntu/data/log_data/"
    s3 = boto3.client("s3")
    bucket = "dsaci"
    # key is the file name – I have just used numbers
    key = 509
    for root, dirs, files in os.walk(dire):
        for filename in files:
            file_path = os.path.join(root, filename)
            with open(file_path, encoding="utf-8") as f:
                for jsonObj in f:
                    dic = json.loads(jsonObj)
                    dic = json.dumps(dic)
                    s3.put_object(Body=dic, Bucket=bucket, Key=str(key))
                    key += 1


# make sure to give a unique (non existing) bucket name
create_bucket("dsaci")
upload_logData()
