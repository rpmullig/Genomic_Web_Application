# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Add utility code here
import boto3
import botocore
from botocore.client import Config
import requests
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import json

# Conect to SQS and get the message from the queue
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.Queue(url=config['aws']['ResultsQueueUrl'])

glacier_client = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])

dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
table = dynamo.Table(config['aws']['AWSDynamodbAnnotationsTable'])

s3 = boto3.resource('s3',
                    region_name=config['aws']['AwsRegionName'],
                    config=Config(signature_version='s3v4'))

# Poll the message queue in a loopn
while True:
    # print(f"Polling SQS with {config['aws']['PollWaitTime']} seconds wait time")
    response = queue.receive_messages(
        WaitTimeSeconds=int(config['aws']['PollWaitTime']),
    )

    if response:
        message_body = json.loads(response[0].body)
        message_data = json.loads(message_body["Message"])
        # print("Message Read in notify.py")
        # print(f'{message_data}')

        glacier_job_id = message_data['JobId']
        job_id = message_data['JobDescription']

        glacier_response = glacier_client.get_job_output(
            vaultName=config['aws']['VaultName'],
            jobId=glacier_job_id,
        )

        data = glacier_response['body'].read()

        # results file aws key placed in description for initiation job in restore.py
        aws_key = message_data['JobDescription']

        # print(aws_key)
        # trying some different stuff
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
        s3.Object(config['aws']['ResultsBucket'], aws_key).put(Body=data)
        # s3.upload_fileobj(data, config['aws']['ResultsBucket'], aws_key)

        # persist update to database
        table.update_item(
            Key={"job_id": aws_key.split('/')[2].split('~')[0]},
            UpdateExpression="SET storage_status = :p",
            ExpressionAttributeValues={':p': "RESTORED"}
        )

        try:
            # print("Deleting notify message now--not now")
            response[0].delete()
        except ClientError as e:
            # print("Failed to delete message from sqs: {}".format(str(e)))
            # print(str(e))
            raise ClientError

### EOF
