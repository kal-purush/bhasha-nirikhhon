import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from immoscout import Immoscout
import decimal
from urllib.parse import unquote

dynamodb = boto3.resource('dynamodb')


def get_default(dict, key, default):
    """ Returns dict[key] if key exists else default. """
    if key in dict and dict[key] is not None:
        return dict[key]
    return default


class DecimalEncoder(json.JSONEncoder):
    """ Converts Decimal to float or integer. """
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def cron(event, context):
    """
    Params
    ------
        event.is_rental: boolean(optional, default=False)
        event.state: string(optional, default=Nordrhein-Westfalen)
        event.city: string(optiona, default=Muenster)
        event.district: string(optional)
    """
    # Get query parameters.
    is_rental = get_default(event, 'is_rental', False)
    state = get_default(event, 'state', 'Nordrhein-Westfalen')
    city = get_default(event, 'city', 'Muenster')
    district = get_default(event, 'district', '')
    # Scrape results.
    scout = Immoscout()
    if is_rental:
        results = scout.rent(state, city, district)
    else:
        results = scout.buy(state, city, district)
    items = [item.json() for item in results.items()]
    # Persist in DynamoDB.
    table = dynamodb.Table('immoscout')
    with table.batch_writer() as batch:
        for item in items:
            data = json.loads(json.dumps(item), parse_float=Decimal)
            batch.put_item(data)
    print(f'Created {len(items)} entries.')
    return 0


def api(event, context):
    """ Query table 'immoscout'.
    Params
    ------
        event.pathParameters.state: string(optional)
        event.pathParameters.city: string(optional)
        event.pathParameters.district: string(optional)
        event.queryStringParameters.limit: int(optional, default=10)
    """
    # Url Parameters
    params = get_default(event, 'pathParameters', {})
    state = get_default(params, 'state', None)
    city = get_default(params, 'city', None)
    district = get_default(params, 'district', None)
    # Query Parameters
    query = get_default(event, 'queryStringParameters', {})
    try:
        limit = min(int(query['limit']), 100)
    except:
        limit = 20
    # Build filter.
    filter_expr = None
    if state:
        filter_expr = Attr('address.state').eq(unquote(state))
    if city:
        filter_expr = filter_expr & Attr('address.city').eq(unquote(city))
    if district:
        filter_expr = filter_expr & Attr('address.district').eq(unquote(district))
    # Scan DynamoDB.
    table = dynamodb.Table('immoscout')
    response = table.scan(FilterExpression=filter_expr, Limit=limit)
    items = response['Items']
    while (response.get('LastEvaluatedKey')) and (limit - len(items) > 0):
        response = table.scan(FilterExpression=filter_expr, ExclusiveStartKey=response['LastEvaluatedKey'], Limit=limit-len(items))
        items.extend(response['Items'])
    # Slice everything after limit.
    items = items[:limit]
    return {
        "statusCode": 200,
        "body": json.dumps({ "count": len(items), "items": items }, cls=DecimalEncoder)
    }