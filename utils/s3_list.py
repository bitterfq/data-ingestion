import os
from dotenv import load_dotenv
import boto3

# Load environment variables from .env file
load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

response = s3.list_objects_v2(Bucket='cdf-upload')
for obj in response.get('Contents', []):
    print(obj['Key'])
