import uuid
import subprocess
import os
import re
import json
import boto3
import botocore
from botocore.client import Config
import requests
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Get util configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

# Conect to SQS and get the message from the queue
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.Queue(url=config['aws']['QueueUrl'])
s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])


def s3_download_file(key, bucket, job_id, filename, username):
    path = os.path.join(os.path.dirname(__file__),  username)
    os.makedirs(path, exist_ok=True)
    # print(f"Path created: {path}")
    try:
        filepath: str = path + '/' + job_id + '~'+ filename
        # print(f'Filepath created {filepath}')
        with open(filepath, 'wb') as data:
            s3.download_file(bucket, key, data.name)
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_fileobj
    except Exception as e:
        # print("Failed to create local folder with job ID of: {} \n{}".format(job_id, str(e)))
        raise e


# Poll the message queue in a loop
while True:
    # Attempt to read a message from teh Queue
    # Use long polloing, here as 20 second wait times
    # print(f"Polling SQS with {config['aws']['PollWaitTime']} seconds wait time")
    response = queue.receive_messages(
        WaitTimeSeconds=int(config['aws']['PollWaitTime']),
    )

    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
    if response:
        message_body = json.loads(response[0].body)
        message_data = json.loads(message_body["Message"])
        # print("Message Read!")
        # print(f'{message_data}')

        job_id = message_data["job_id"]
        filename = message_data["input_file_name"]
        bucket = message_data["s3_inputs_bucket"]
        key = message_data["s3_key_input_file"]
        username = key.split('/')[1]

        s3_download_file(str(key), str(bucket), str(job_id), str(filename), str(username))
        # print(f'Download path from s3: {username} with job_ID: {job_id}')

        try:
            # Launch annotation job as a background process
            # print("launching annotator with Popen")
            ann_process = subprocess.Popen([
                'python',
                '{}/run.py'.format(os.path.dirname(__file__)),
                '{}/{}/{}~{}'.format(os.path.dirname(__file__), username, job_id, filename),
            ],
            )
        except Exception as e:
            # print("Failed to run subprocess run.py: {}".format(str(e)))
            raise e

        dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        annotations = dynamo.Table(config['aws']['AWSDynamodbAnnotationsTable'])
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-long-polling.html
            # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#updating-item
            # print("Uploading to dynamodb with new status")
            annotations.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET job_status = :status",
                ConditionExpression=Attr('job_status').eq("PENDING"),
                ExpressionAttributeValues={
                    ':status': "RUNNING",
                }
            )
        except dynamo.meta.client.exceptions.ConditionalCheckFailedException as e:
            # print("Failed to connect and update dynamodb: {}".format(str(e)))
            raise e

        try:
            # print("Deleting Message now")
            response[0].delete()
        except Exception as e:
            # print("Failed to delete message from sqs: {}".format(str(e)))
            raise e
