import boto3
import logging
from time import sleep
from botocore.exceptions import ClientError

client = boto3.client('ec2')


def lambda_handler(event, context):
    # 削除された対象のAMI IDを取得
    image_id = event['detail']['requestParameters']['imageId']

    # 削除対象のスナップショットを取得
    response = client.describe_snapshots(
        Filters=[
            {
                'Name': 'description',
                'Values': [
                    'Created by CreateImage(*) for ' + image_id + ' from *',
                ]
            }
        ]
    )

    # スナップショットの削除
    for i in response['Snapshots']:
        try:
            return client.delete_snapshot(SnapshotId=i['SnapshotId'])
        except ClientError as e:
            logging.error("Delete snapshot error: %s", e)