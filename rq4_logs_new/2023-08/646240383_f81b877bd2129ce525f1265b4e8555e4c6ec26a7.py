from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import DESCENDING

from bson import json_util

from config import Config

config = Config()

client = MongoClient(config.db_connection_string,
                     server_api=ServerApi(version='1'))
db = client[config.db_name]
collection = db[config.collection_name]


def lambda_handler(event, context):
    """
    Lambda function to get the latest model performance metrics.

    :param event: Event data passed to the function.
    :type event: dict
    :param context: Runtime information provided by AWS Lambda.
    :type context: LambdaContext

    :return: The latest model performance metrics and status code.
    :rtype: dict
    """

    last_date = dict(collection.find_one(
        sort=[('workflow_date', DESCENDING)]
    ))['workflow_date']
    
    records = list(
        collection.find(
            {
                'workflow_date': last_date
            }

        )
    )

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json_util.dumps(records)
    }