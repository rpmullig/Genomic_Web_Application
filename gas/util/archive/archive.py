# archive.py
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
config.read('archive_config.ini')

# Add utility code here
import boto3
import botocore
from botocore.client import Config
import requests
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import json
from helpers import send_email_ses, get_user_profile

# Conect to SQS and get the message from the queue
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue
sqs = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])
queue = sqs.Queue(url=config['aws'][
    'ArchiveQueueUrl'], )  # 5 minutes delay https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
queue.set_attributes(Attributes={'DelaySeconds': '300'}, )

# Poll the message queue in a loop
while True:
    # Attempt to read a message from teh Queue
    # Use long polloing, here as 20 second wait times
    # print(f"Polling SQS with {config['aws']['PollWaitTime']} seconds wait time")
    response = queue.receive_messages(
        WaitTimeSeconds=int(config['aws']['PollWaitTime']),
    )

    if response:
        message_body = json.loads(response[0].body)
        message_data = json.loads(message_body["Message"])
        # print("Message Read in archive.py")
        # print(f'{message_data}')

        job_id = message_data["job_id"]
        input_file_name = message_data["input_file_name"]
        user_id = message_data["user_id"]

        # persist to DynamoDB if free user
        try:

            profile = get_user_profile(id=user_id)  # very uncertain about this being proper form

            if profile[4] == 'free_user':
                # print("Free user archiving")
                glacier_client = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
                dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])

                aws_key = config['aws']['AWSS3Prefix'] + str(profile[0]) + '/' + str(
                    job_id) + '~' + input_file_name + '.annot'

                s3 = boto3.client('s3',
                                  region_name=config['aws']['AwsRegionName'],
                                  config=Config(signature_version='s3v4'))

                obj = s3.get_object(Bucket=config['aws']['AWSS3ResultsBucket'], Key=aws_key)
                body = obj['Body'].read().decode('utf-8')

                glacier_response = glacier_client.upload_archive(vaultName=config['aws']['AWSGlacierVault'],
                                                         body=body)
                glacier_id = glacier_response['archiveId']
                # print("Glacier response: {}", glacier_response)

                data = {
                    'results_file_archive_id': glacier_id,
                    "job_id": str(job_id),
                    "user_id": str(profile[0]),
                    "input_file_name": str(input_file_name),
                    "storage_status": "ARCHIVED"
                }

                # print("data: from successful free_user: {}", data)
                table = dynamo.Table(config['aws']['AWSDynamodbAnnotationsTable'])

                # print("Updating item in table dynammoDB")
                table.update_item(
                    Key={"job_id": str(job_id)},
                    UpdateExpression="SET results_file_archive_id = :a, storage_status= :b",
                    ExpressionAttributeValues={
                        ':a': glacier_id,
                        ':b': "ARCHIVED",
                    }
                )
                # print("Deleting S3 file from results bucket")
                obj = s3.delete_object(Bucket=config['aws']['AWSS3ResultsBucket'], Key=aws_key)

        except ClientError as e:
            # print("Failed to get profile and/or persist to DynamoDB: {}".format(str(e)))
            # print(str(e))
            raise ClientError

        try:
            # print("Deleting archive notification message from sqs")
            response[0].delete()
        except ClientError as e:
            # print("Failed to archive notification message from sqs: {}".format(str(e)))
            # print(str(e))
            raise ClientError

### EOF
