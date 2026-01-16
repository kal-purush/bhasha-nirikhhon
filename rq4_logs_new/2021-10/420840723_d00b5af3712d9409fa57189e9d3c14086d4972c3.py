import json
import os
import boto3

table_name = os.environ['TABLE_NAME']
dynamodb = boto3.client("dynamodb")

def lambda_handler(event, context):

    queryStringParams = event["queryStringParameters"]
    try:
        date = queryStringParams["date"]
    except:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "date query parameter is required."
            })
        }

    entries = dynamodb.query(
        TableName=table_name,
        KeyConditionExpression='SetDate = :d',
        ExpressionAttributeValues={
            ':d': {'S': queryStringParams["date"]}
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": entries
        })
    }