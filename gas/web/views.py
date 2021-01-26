# views.py
#
# Robert Mulligan, MPCS Cloud Computing 
# University of Chicago
#
# Application logic for the GAS
#
##

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
                   request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = boto3.client('s3',
                      region_name=app.config['AWS_REGION_NAME'],
                      config=Config(signature_version='s3v4'))

    bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
    user_id = session['primary_identity']

    # Generate unique ID to be used as S3 key (name)
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
               str(uuid.uuid4()) + '~${filename}'

    # Create the redirect URL
    redirect_url = str(request.url) + '/job'

    # Define policy fields/conditions
    encryption = app.config['AWS_S3_ENCRYPTION']
    acl = app.config['AWS_S3_ACL']
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl}
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get('bucket'))
    s3_key = str(request.args.get('key'))
    # print(s3_key)

    # Extract the job ID from the S3 key
    job_id = s3_key.split('/')[2]
    job_id = job_id.split('~')[0]

    app.logger.info(f"job_id from redirect s3_key for annotation job request listed as: {job_id}")
    filename = s3_key.split('~')[1]

    # Create a job item and persist it to the annotations database
    profile = get_profile(identity_id=session.get('primary_identity'))
    data = {"job_id": job_id,
            "user_id": str(profile.identity_id),
            "input_file_name": filename,
            "s3_inputs_bucket": bucket_name,
            "s3_key_input_file": s3_key,
            "submit_time": int(time.time()),  # per course slides, epoch time
            "job_status": "PENDING",
            }

    # Persist job to database
    try:
        dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        annotations = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        annotations.put_item(Item=data)
        app.logger.info(
            f"Successful persisted job to dynamoDB by {profile.name} (Globus identity: {profile.identity_id})")
    except ClientError as e:
        app.logger.info(
            f"Unsuccessful persisted job to dynamoDB by {profile.name} (Globus identity: {profile.identity_id})")
        return json.dumps({'code': 500, 'status': 'error',
                           'message': f'Failed to save job in database: {e}'})

    # Send message to request queue
    json_message = json.dumps({"default": json.dumps(data)})
    snsClient = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    try:
        ann_job_response = snsClient.publish(
            TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
            MessageStructure='json',
            Message=json_message)
        app.logger.info(
            f"Successful message request to SNS queue by {profile.name} (Globus identity: {profile.identity_id})")
    except ClientError as e:
        app.logger.info(
            f"Unsuccessful message request to SNS queue by {profile.name} (Globus identity: {profile.identity_id})")
        return json.dumps({'code': 500, 'status': 'error',
                           'message': f'Failed to submit job on SNS:: {e}'})

    if ann_job_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        return json.dumps({'code': 500, 'status': 'error',
                           'message': "Failed to submit job on SNS: {}".format(ann_job_response)})

    return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""


@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    # Get list of annotations to display
    profile = get_profile(identity_id=session['primary_identity'])
    try:
        dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
        query_dict = table.query(
            IndexName="user_id_index",
            KeyConditionExpression=Key('user_id').eq(str(profile.identity_id)),
        )

    except ClientError as e:
        app.logger.info(
            f"Unsuccessful access of dynamodb by {profile.name} (Globus identity: {profile.identity_id})")
        return json.dumps({'code': 500, 'status': 'error',
                           'message': f'Failed to access database: {e}'})

    annotations = list()

    for item in query_dict['Items']:
        t = time.localtime(item['submit_time'])
        time_str = '{}-{}-{} {}:{}'.format(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min)

        annotations.append({
            'job_id': item['job_id'],
            'input_file_name': item['input_file_name'],
            'job_status': item['job_status'],
            'submit_time': time_str,

        })

    return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""


@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    profile = get_profile(identity_id=session['primary_identity'])
    try:
        dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
        query_dict = table.query(
            KeyConditionExpression=Key('job_id').eq(str(id)),
        )
    except ClientError as e:
        app.logger.info(
            f"Unsuccessful access of dynamodb by {profile.name} (Globus identity: {profile.identity_id})")
        return json.dumps({'code': 500, 'status': 'error',
                           'message': f'Failed to access database: {e}'})

    if query_dict['Items'][0]['user_id'] != str(profile.identity_id):
        return json.dumps({'code': 500, 'status': 'error',
                           'message': f'Not authorized to view this job'})
    else:
        annotation = query_dict['Items'][0]

        free_access_expired = False  # default value for #8

        # fix the date formatting
        t = time.localtime(annotation['submit_time'])
        time_str = '{}-{}-{} {}:{}'.format(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min)
        annotation['submit_time'] = time_str
        if annotation['job_status'] == 'COMPLETED':

            # problem #8
            # check the role and if the user has submitted within 5 minutes -- if not, push premium account and no download
            # set default time under 5 minutes if it's not completed
            if profile.role == "free_user" and int(time.time()) - int(annotation['complete_time']) >= 300:
                app.logger.info("Access by a free user")
                free_access_expired = True

            t = time.localtime(annotation['complete_time'])
            time_str = '{}-{}-{} {}:{}'.format(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min)
            annotation['complete_time'] = time_str

            # download link
            s3 = boto3.client('s3',
                              region_name=app.config['AWS_REGION_NAME'],
                              config=Config(signature_version='s3v4'))
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
                # https://aws.amazon.com/premiumsupport/knowledge-center/presigned-url-s3-bucket-expiration/
                annotation['result_file_url'] = s3.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={
                        'Bucket': annotation['s3_results_bucket'],
                        'Key': annotation['s3_key_result_file']
                    }, )  # note, using default expiration of 3600 seconds (one hour)
            except ClientError as e:
                app.logger.error(f"Unable to generate presigned URL for upload: {e}")
                return abort(500)

        return render_template('annotation_details.html', annotation=annotation,
                               free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""


@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    # There must be a better way than to run another dynamodb query for the log file, but I could not
    # get the wildcard working to read a file from the key name given <aws_prefix>/<user>/<id>~*.count.log as the key
    profile = get_profile(identity_id=session['primary_identity'])
    try:
        dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
        query_dict = table.query(
            KeyConditionExpression=Key('job_id').eq(str(id)),
        )
    except ClientError as e:
        app.logger.info(
            f"Unsuccessful access of dynamodb by {profile.name} (Globus identity: {profile.identity_id})")
        return json.dumps({'code': 500, 'status': 'error',
                           'message': f'Failed to access database: {e}'})

    if query_dict['Items'][0]['user_id'] != str(profile.identity_id):
        return json.dumps({'code': 500, 'status': 'error',
                           'message': f'Not authorized to view this job'})
    else:
        annotation = query_dict['Items'][0]

        s3 = boto3.client('s3',
                          region_name=app.config['AWS_REGION_NAME'],
                          config=Config(signature_version='s3v4'))

        # https://www.edureka.co/community/17558/python-aws-boto3-how-do-i-read-files-from-s3-bucket
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object

        obj = s3.get_object(
            Bucket=str(annotation['s3_results_bucket']),
            Key=str(annotation['s3_key_result_file']),
        )
        body = obj['Body'].read().decode('utf-8')

        return render_template('view_log.html', log_file_contents=body, job_id=id)


"""Subscription management handler
"""
import stripe


@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    if (request.method == 'GET'):
        # Display form to get subscriber credit card info
        if (session.get('role') == "free_user"):
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif (request.method == 'POST'):
        # Process the subscription request
        token = str(request.form['stripe_token']).strip()

        # Create a customer on Stripe
        stripe.api_key = app.config['STRIPE_SECRET_KEY']
        try:
            customer = stripe.Customer.create(
                card=token,
                plan="premium_plan",
                email=session.get('email'),
                description=session.get('name')
            )
        except Exception as e:
            app.logger.error(f"Failed to create customer billing record: {e}")
            return abort(500)

        # Update user role to allow access to paid features
        update_profile(
            identity_id=session['primary_identity'],
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
        # Make sure you handle files not yet archived!
        data = {
            'user_id': session['primary_identity'],
            'thaw_status': 'PENDING'
        }

        json_message = json.dumps({"default": json.dumps(data)})
        snsClient = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])

        ann_job_response = snsClient.publish(
            TopicArn=app.config['AWS_SNS_JOB_RESTORE_TOPIC'],
            MessageStructure='json',
            Message=json_message,
        )
        app.logger.info(f"Successful restore request to SNS")

        # Display confirmation page
        return render_template('subscribe_confirm.html',
                               stripe_customer_id=str(customer['id'])
                               )


"""Reset subscription
"""


@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""


@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')


"""Login page; send user to Globus Auth
"""


@app.route('/login', methods=['GET'])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if (request.args.get('next')):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html',
                           title='Page not found', alert_level='warning',
                           message="The page you tried to reach does not exist. \
      Please check the URL and try again."
                           ), 404


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html',
                           title='Not authorized', alert_level='danger',
                           message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
                           ), 403


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return render_template('error.html',
                           title='Not allowed', alert_level='warning',
                           message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
                           ), 405


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
                           title='Server error', alert_level='danger',
                           message="The server encountered an error and could \
      not process your request."
                           ), 500

### EOF
