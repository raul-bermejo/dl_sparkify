import boto3
import json
import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

AWS_KEY_ID                    = config.get('AWS','AWS_KEY_ID')
AWS_SECRET                 = config.get('AWS','AWS_SECRET')


# Initialize AWS resources using AWS Python SDK (boto3)
s3 = boto3.resource('s3',
                    region_name='us-west-2',
                    aws_access_key_id=AWS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET)


# Extract list of current s3 buckets
s3 = boto3.client('s3',
                    region_name='us-west-2',
                    aws_access_key_id=AWS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET)

response = s3.list_buckets()
bucket_list = response["Buckets"]

print(bucket_list)
