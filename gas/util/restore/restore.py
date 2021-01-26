# restore.py
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
config.read('restore_config.ini')

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
queue = sqs.Queue(url=config['aws']['RestoreQueueUrl'])
glacier_client = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
snsClient = boto3.client('sns', region_name=config['aws']['AwsRegionName'])

dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
table = dynamo.Table(config['aws']['AWSDynamodbAnnotationsTable'])

# Poll the message queue in a loopn
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
        # print("Message Read in restore.py")
        # print(f'{message_data}')

        user_id = message_data["user_id"]
        thaw_status = message_data["thaw_status"]

        # print(f'User {user_id} bought premium. Status {thaw_status}')

        query_dict = table.scan(
            FilterExpression=Attr('user_id').eq(user_id) & Attr('storage_status').eq('ARCHIVED'),
        )
        # note: the archive process immediately specifies 'ARCHIVED' fpr stprage_status
        # and it only archives for free users. If A user converted to premium under 5 minutes of a job
        # nothing happens. If a user converted to premium while one is being archived,
        # it will show up and be put into this query dictionary for retrieval

        # print(f'\n\n\nPrinting query dictionary: \n\n\n {query_dict}\n\n\n')

        for item in query_dict['Items']:

            try:
                # print("trying data retrieval expedited")
                # docs.aws.amazon.com/amazonglacier/latest/dev/api-initiate-job-post.html
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
                glacier_response = glacier_client.initiate_job(
                    accountId='-',
                    vaultName=config['aws']['VaultName'],
                    jobParameters={
                        'Description': item['s3_key_result_file'],
                        'SNSTopic': config['aws']['ThawARN'],
                        'ArchiveId': item['results_file_archive_id'],
                        'Type': "archive-retrieval",
                        'Tier': 'Expedited',
                    }
                )
                # print(response)
            except ClientError as e:
                try:
                    # print("Failed data retrieval expedited, trying standard")
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
                    glacier_response = glacier_client.initiate_job(
                        accountId='-',
                        vaultName='ucmpcs',
                        jobParameters={
                            'Description': item['s3_key_result_file'],
                            'SNSTopic': config['aws']['ThawARN'],
                            'ArchiveId': str(item['results_file_archive_id']),
                            'Type': "archive-retrieval",
                            'Tier': 'Standard',
                        }
                    )
                    # print(response)
                except ClientError as e:
                    # print("Unable to retrieve form archive")
                    raise e

        try:
            # print("Deleting notify message now--not now")
            response[0].delete()
        except ClientError as e:
            # print("Failed to delete message from sqs: {}".format(str(e)))
            # print(str(e))
            raise ClientError
### EOF
