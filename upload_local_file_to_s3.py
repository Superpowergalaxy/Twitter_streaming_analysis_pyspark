from twitter_credentials import ACCESS_ID, ACCESS_KEY
import boto3
import logging
from botocore.exceptions import ClientError


def upload_file(ACCESS_ID,ACCESS_KEY,bucket,file_name,object_name = None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3',
        aws_access_key_id=ACCESS_ID,
        aws_secret_access_key=ACCESS_KEY)

    try:
        response = s3_client.upload_file(file_name, bucket,object_name)
        print('upload_successful\n', response)
    except ClientError as e:
        logging.error(e)
        # print(e)
        return False
    return True

object_name = input().strip() 
upload_file(ACCESS_ID,ACCESS_ID,'twitter-bucket-jingyusu','stream_covid-19.csv',object_name)