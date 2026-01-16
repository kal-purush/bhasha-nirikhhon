from os import environ

import boto3

from hyp3_sdk import HyP3

DB = boto3.resource('dynamodb')


def harvest(product):
    pass


def is_succeeded(product):
    return False


def get_products():
    table = DB.Table(environ['PRODUCT_TABLE'])
    response = table.scan()
    products = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey'],
        )
        products.extend(response['Items'])
    return products

def lambda_handler(event, context):
    products = get_products()
    for product in products:
        if is_succeeded(products):
            harvest(product)