import json
import logging
import os
import uuid
import boto3
from botocore.config import Config

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))
_logger = logging.getLogger(__name__)
AWS_CONFIG = Config(retries={'max_attempts': 10})

BUSINESS_TABLE_NAME = os.environ.get('BUSINESS_TABLE_NAME')
dynamodb = boto3.resource('dynamodb', config=AWS_CONFIG)
dynamodb = dynamodb.Table(BUSINESS_TABLE_NAME)
#uuid=uuid.uuid4(uuid.NAMESPACE_DNS, 'python.org')


# def insert_item_into_db(item):
#     print("------------------INSERT ITEM-----------------")
#     dynamodb.put_item(
#         TableName=BUSINESS_TABLE_NAME,
#         Item=item
#     )


def lambda_handler(event, context):
    _logger.debug('Event received: {}'.format(json.dumps(event)))
    print("-------------------EVENT-----------------------")
    print(event)
    
    # HANTZ - add your python code below here
    
    # item = json.loads(event.get('body'))
    # item["BusinessId"] = str(uuid.uuid4())

    # insert_item_into_db(item)
    # response = {
    #     'statusCode': 200,
    #     'body': json.dumps(
    #         {'success': True,
    #          "message": item}
    #     )
    # }
    # _logger.debug('responseonse: {}'.format(json.dumps(response)))
    return response