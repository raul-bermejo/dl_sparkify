import boto3
import json
import configparser
from botocore.exceptions import ClientError

# Read AWS credentials from .cfg file
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

AWS_KEY_ID = config.get('AWS','AWS_KEY_ID')
AWS_SECRET = config.get('AWS','AWS_SECRET')

region = 'us-west-2'

# Create Python client to use AWS API
s3 = boto3.client('s3',
                  region_name=region,
                  aws_access_key_id=AWS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET)

# Create s3 bucket and upload zipped files from data dir (only once)
create_bucket = False
upload_files = False

bucket_name = 'dl-sparkify'
files = ['log-data.zip', 'song-data.zip']

if create_bucket:
    location = {'LocationConstraint': region}
    s3.create_bucket(Bucket=f'{bucket_name}',
                     CreateBucketConfiguration=location)
    print(f"The bucket {bucket_name} was created succesfully. \n", "="*60)

if upload_files:
    for filename in files:
        s3.upload_file(f"./data/{filename}", bucket_name, filename)
    print(f"The files were uploaded into {bucket_name} bucket succesfully. \n", "="*60)

# Print existing buckets
response = s3.list_buckets()
bucket_list = [bucket['Name'] for bucket in response["Buckets"]]
print("Current Buckets:", bucket_list)