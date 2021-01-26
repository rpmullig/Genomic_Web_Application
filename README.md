# Robert Mulligan MPCS - Cloud Computing Capstone

# Archive Process
The key to my archive process was the following: 
```python
queue.set_attributes(Attributes={'DelaySeconds': '300'}, )
```
I delayed the SQS by 5 minutes by setting this attribute. I chose to utilize SQS and SNS to decouple and create
an asychronous process, which will help in the event of a network partition or failures.

The process persists the job in DynamoDB with a storage status as "archived", and this happens to 

# Restore Process

I utilized SQS/SNS for the restore process. For the same reasons as archive: this decouples and runs asychrounsly
to counter network partitions or node failures

### views.py 

Whe a user subscribes, an SNS is sent to the restore SQS.


### Restore.py 

Reads the SQS from views sns and scans all the jobs of that user listed as "archived" in dynamoDB. 
Critically, I utilize initiate job and *pass the AWS results key via the description. 

### Thaw.py

Thaw runs a long poll for the SNS that was attachd to glacier's Initiate job. This will run the s3.download
and upload to the results bucket--which it finds from the description passed into Glacier's `initiate_job()` 


# Notes for Grader

All of my testing passed for the utilities, so if something does not work--please review the code for possible minor
edge case error. I did not do any extra credit due to time constraints.

The `project_tracking` is just a folder for personal use of this project later--it's not relevant to anything grading related.

I had modified the code paths in `run_gas.sh` to include parent root directory `cp-rpmullig` and as such have saved only 
one file in the s3 bucket used for autoscaling as `cp-rpmullig.zip`--Vas approved of this. 

Please accept the commented print statements as guiding code comments as well--because I used them as such. 


