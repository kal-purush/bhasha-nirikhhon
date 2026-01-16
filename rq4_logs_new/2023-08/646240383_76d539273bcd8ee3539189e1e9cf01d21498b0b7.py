import boto3
import botocore
import json


def get_data_from_s3(bucket_name: str, s3_path: str) -> any:
    """
    Get data from S3.

    :param bucket_name: Name of S3 bucket.
    :type: str
    :param s3_path: Path to data.
    :type: str

    :raises: botocore.exceptions.ClientError

    :return: Fetched data.
    :rtype: json
    """

    s3_client = boto3.client('s3')
    try:
        return json.load(s3_client.get_object(Bucket=bucket_name, Key=s3_path)['Body'])
    except botocore.exceptions.ClientError as error:
        raise error