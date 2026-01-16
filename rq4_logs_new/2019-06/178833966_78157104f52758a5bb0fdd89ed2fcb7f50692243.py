import os
import aioboto3
import boto3
from utils.lambda_decorators import ssm_parameters, async_handler
from utils.json_serialisation import dumps
from json import loads
from utils.objectify_dict import objectify
import tarfile
import re
import io
import datetime
from urllib.parse import unquote_plus
import importlib.util
from utils.scan_results import ResultsContext


class ResultsParser:
    region = os.environ["REGION"]
    app_name = os.environ["APP_NAME"]
    task_name = os.environ["TASK_NAME"]
    stage = os.environ["STAGE"]
    ssm_prefix = f"/{app_name}/{stage}"
    ssm_client = boto3.client("ssm", region_name=region)
    SNS_TOPIC = f"{ssm_prefix}/tasks/{task_name}/results/arn"
    s3_client = boto3.client("s3", region_name=region)
    sns_client = boto3.client("sns", region_name=region)

    def get_ssm_params(self, ssm_client, *param_names):
        ret = self.ssm_client.get_parameters(Names=[*param_names])
        params = ret['Parameters']
        return {p['Name']: p['Value'] for p in params}

    def __init__(self, event):
        self.event = event

    def post_results(self, topic, doc_type, document):
        r = self.sns_client.publish(
            TopicArn=topic, Subject=doc_type, Message=dumps(document)
        )
        print(f"Published message {r['MessageId']}")

    def process_results(self, topic, bucket, key):
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)

        # extract the message data from the S3 Metadata, and remove the suffix added to the keys by boto3
        metadata = obj["Metadata"]
        msgdata = {'records': []}
        for metakey in metadata:
            # strip out the header that AWS adds
            msgdata[metakey.replace('x-amz-meta-', '')] = metadata[metakey]

        # extract the results from the tar file
        content = obj["Body"].read()
        tar = tarfile.open(mode="r:gz", fileobj=io.BytesIO(content), format=tarfile.PAX_FORMAT)
        result_file_name = re.sub(r"\.tar.gz$", "", key.split("/", -1)[-1])
        body = tar.extractfile(result_file_name).read().decode('utf-8').split('\n')

        # we need these to track the scan (TODO: this will be removed and replaced by a better identifier later)
        start_time, end_time = msgdata["scan_end_time"], msgdata["scan_end_time"]
        scan_id = os.path.splitext(result_file_name)[0]

        # set up the base set of keys to send to elastic
        non_temporal_key = {
            "address": msgdata['address'],
            "address_type": msgdata['address_type']
        }
        # TODO store start_time correctly
        # TODO end_time will be correct once the scheduler is in place to change the key (currently end_time is used)

        results_context = ResultsContext(topic, non_temporal_key, scan_id, start_time,
                                         end_time, self.task_name, self.sns_client)
        if self.func_iterateresults != None:
            self.func_iterateresults(results_context, body, TaskTopic=topic, TaskBucket=bucket, TaskKey=key)


    def start(self, PreProcessRecord=None, IterateResults=None):
        # Pass in variables by name for:
        # PreProcessRecord=func() - optional
        # IterateResults=func() - mandatory
        self.event['ssm_params'] = self.get_ssm_params(self.ssm_client, self.SNS_TOPIC)
        topic = self.event['ssm_params'][self.SNS_TOPIC]
        self.func_preprocess = PreProcessRecord
        self.func_iterateresults = IterateResults
        for record in self.event["Records"]:
            s3_object = objectify(record["s3"])
            bucket = s3_object.bucket.name
            key = unquote_plus(s3_object.object.key)
            if self.func_preprocess != None:
                (topic, bucket, key) = self.func_preprocess(topic, bucket, key)
            self.process_results(topic, bucket, key)