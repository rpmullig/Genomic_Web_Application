# notify.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
import os
import sys

# Import utility helpers


sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers
from helpers import send_email_ses, get_user_profile

# Get configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

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
        # print("Message Read in notify.py")
        # print(f'{message_data}')

        job_id = message_data["job_id"]
        input_file_name = message_data["input_file_name"]
        user_id = message_data["user_id"]

        try:
            # print(f"Sending email")
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ses.html
            profile = get_user_profile(id=user_id)  # very uncertain about this being proper form
            # print(profile)
            send_email_ses(recipients=profile[2],  # this is not the best form, there should be a better way
                           sender=config['aws']['EmailDefaultSender'],
                           subject=config['aws']['EmailSubject'],
                           body=config['aws']['EmailBodyOne'] + ' ' + str(job_id) + ' ' + config['aws']['EmailBodyTwo'])
        except Exception as e:
            # print("Failed to send emails")
            # print(str(e))
            raise e

        try:
            # print("Deleting notify message now")
            response[0].delete()
        except ClientError as e:
            # print("Failed to delete message from sqs: {}".format(str(e)))
            # print(str(e))
            raise ClientError
### EOF
