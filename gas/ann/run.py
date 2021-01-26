# run.py
#
#
# Wrapper script for running AnnTools
#
import sys
import time
import driver
import os
import boto3
import shutil
import json
from botocore.client import Config
from botocore.exceptions import ClientError

# Get util configuration
from configparser import SafeConfigParser

config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

        # print(f'The input to run.py looks like this: {sys.argv[1]}')
        # get the directory name from inputs [this needs to be cleaned up!]
        directory_name = os.path.abspath(os.path.dirname(sys.argv[1]))
        input_path = sys.argv[1].split('/')
        path_items = len(input_path)
        filename = input_path[path_items - 1]
        job_id = input_path[path_items - 2].replace('/', '')
        user_id = input_path[-2]

        # catch if there's an error reading those string parsed inputs above (if they're empty)
        if not input_path or not path_items or not filename or not job_id:
            # print("Could not read input strings for run.py -- found empty string")
            raise ValueError

        try:
            s3 = boto3.client('s3',
                              region_name=config['aws']['AwsRegionName'],
                              config=Config(signature_version=config['aws']['SignatureVersion']))

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
            # upload results file
            # print("Attempting to upload: {}/{}.annot".format(directory_name, filename))
            s3.upload_file("{}/{}.annot".format(directory_name, filename),
                           '{}'.format(config['aws']['AWSS3ResultsBucket']),
                           '{}/{}/{}.annot'.format(config['aws']['AWSS3Prefix'], user_id, filename))
            # upload log file
            # print("Attempting to upload: {}/{}.count.log".format(directory_name, filename))
            s3.upload_file("{}/{}.count.log".format(directory_name, filename),
                           '{}'.format(config['aws']['AWSS3ResultsBucket']),
                           '{}/{}/{}.count.log'.format(config['aws']['AWSS3Prefix'], user_id, filename))
        except ClientError as e:
            # print("Error in uploading s3 files to the gas-results: {}".format(str(e)))
            raise e

        try:
            # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
            dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
            annotations = dynamo.Table(config['aws']['AWSDynamodbAnnotationsTable'])

            annotations.update_item(
                Key={"job_id": filename.split('~')[0]},
                UpdateExpression="SET job_status = :p",
                ExpressionAttributeValues={':p': "COMPLETED"}
            )

            annotations.update_item(
                Key={"job_id": filename.split('~')[0]},
                UpdateExpression="SET s3_results_bucket = :a, complete_time = :b, \
                    s3_key_log_file = :c, s3_key_result_file = :d",
                ExpressionAttributeValues={
                    ':a': config['aws']['AWSS3ResultsBucket'],
                    ':b': int(time.time()),
                    ':c': '{}/{}/{}.count.log'.format(config['aws']['AWSS3Prefix'], user_id, filename),
                    ':d': '{}/{}/{}.annot'.format(config['aws']['AWSS3Prefix'], user_id, filename),
                })
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
        except ClientError as e:
            # print("Error in updating dynamobd: {}".format(str(e)))
            raise e

            # now notify the user problem #3
        try:
            data = {'job_id': filename.split('~')[0],
                    "input_file_name": filename.split('~')[1],
                    "user_id": directory_name.split('/')[-1],
                    }
            json_message = json.dumps({"default": json.dumps(data)})
            snsClient = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
            ann_job_response = snsClient.publish(
                TopicArn=config['aws']['AwsSNSJobResultsTopic'],
                MessageStructure=config['aws']['MessageStructure'],
                Message=json_message)
            # print(f"Successful message results to SNS queue after finishing run.py operations")
        except ClientError as e:
            # print(f"Unsuccessful message results to SNS queue after finishing run.py operations")
            # print(f'{str(e)}')
            raise ClientError

        # problem #7 create an archive
        # will send to archive SNS regardless and user type (free vs premium) checked in archive.py
        try:
            data = {'job_id': filename.split('~')[0],
                    "input_file_name": filename.split('~')[1],
                    "user_id": directory_name.split('/')[-1],
                    }
            json_message = json.dumps({"default": json.dumps(data)})
            snsClient = boto3.client('sns', region_name=config['aws']['AwsRegionName'])

            ann_job_response = snsClient.publish(
                TopicArn=config['aws']['AwsSNSJobArchiveTopic'],
                MessageStructure=config['aws']['MessageStructure'],
                Message=json_message,
            )
            # print(f"Successful message results to archive SNS queue after finishing run.py operations")
        except ClientError as e:
            # print(f"Unsuccessful message results to  archive SNS queue after finishing run.py operations")
            # print(f'{str(e)}')
            raise ClientError

        try:
            # delete the directory
            shutil.rmtree("{}/{}/".format(os.path.dirname(__file__), job_id))
        except Exception as e:
            # print("Error while deleting file on instance {}{}/".format(os.path.dirname(__file__), job_id))
            # print(f"{str(e)}")
            raise e


    # else:
    #     print("A valid .vcf file must be provided as input to this program.")

### EOF
