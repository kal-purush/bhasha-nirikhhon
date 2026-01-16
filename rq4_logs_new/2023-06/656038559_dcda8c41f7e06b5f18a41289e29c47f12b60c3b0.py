import os
import json
import boto3
from matches.matchHighlights import createHighlights, getHighlights, getMatch
from utils import getTeamsFromQueryParams

from universes.pokemon import getPokemonTeam
from universes.starwars import getStarWarsTeam

# If we run with localstack, we should use the built in DynamoDB endpoint
if 'LOCALSTACK_HOSTNAME' in os.environ:
    dynamodb_endpoint = 'http://%s:4566' % os.environ['LOCALSTACK_HOSTNAME']
    dynamodb_client = boto3.client('dynamodb', endpoint_url=dynamodb_endpoint)
else:
    dynamodb_client = boto3.client('dynamodb')

# Get a reference to the DynamoDB table
TEAMS_TABLE = os.environ['TEAMS_TABLE']

# The event object has a query param of the teams universe (like 'pokemon' or 'starwars') can contain multiple teams as a comma seperated list
def getTeams(event, context):
    # Start by getting the query params from the request
    queryKeys = event['queryStringParameters']

    # Extract the teams from the query params
    teamsQueryKeys = getTeamsFromQueryParams(queryKeys)

    if len(teamsQueryKeys) == 0:
        return {
            'statusCode': 400,
            'body': json.dumps('No teams provided')
        }

    # Get the teams from DynamoDB
    # BatchGet by pk and sk by passing in the queryKeys
    results = dynamodb_client.batch_get_item(
        RequestItems={
            TEAMS_TABLE: {
                'Keys': [
                    {
                        'pk': {'S': 'team'},
                        'sk': {'S': team}
                    } for team in teamsQueryKeys
                ]
            }
        }
    )

    if len(results['Items']) == 0:
        return {
            'statusCode': 404,
            'body': 'Teams not found'
        }
    
    # Get the teams from the Items
    teams = list(map(lambda item: item['team']['M'], results['Items']))

    # Return the teams
    return {
        'statusCode': 200,
        'body': json.dumps(teams)
    }


# The event object has a path parameter of the team universe (like 'pokemon' or 'starwars')
def getTeam(event, context):
    queryKey = event['pathParameters']['team']

    if queryKey == '' or queryKey is None:
        return {
            'statusCode': 400,
            'body': json.dumps('No team provided')
        }

    # Get the teams from DynamoDB
    # Query by pk by passing in the queryKey
    results = dynamodb_client.query(
        TableName=TEAMS_TABLE,
        KeyConditionExpression='pk = :pk, sk = :sk',
        ExpressionAttributeValues={
            ':pk': {
                'S': 'team'
            },
            ':sk': {
                'S': queryKey
            },
        }
    )

    if len(results['Items']) == 0:
        return {
            'statusCode': 404,
            'body': 'Team not found'
        }
    
    # This would be a great place to validate the items coming from DynamoDB
    # ie. is there 1 goalie, 2 defenders and 2 attackers?

    # Return the teams
    return {
        'statusCode': 200,
        'body': json.dumps(results['Items'][0]["team"]["M"])
    }


# This function returns a list of "events" that occured during a match between to teams
# This could also be done via DynamoDB Streams when a "match" is concluded - and saved to DynamoDB to be directly queried
def getMatchHighlights(event, context):
    matchId = event['pathParameters']['matchId']

    if matchId == '' or matchId is None:
        return {
            'statusCode': 400,
            'body': json.dumps('No team provided')
        }
    
    match = getMatch(matchId)

    # Bit of bad naming here - If time allowed it, i would probably raise an exception and catch it instead
    if match['statusCode'] != 200:
        return match

    highlights = createHighlights(match)

    # Return the highlights
    return {
        'statusCode': 200,
        'body': json.dumps(highlights)
    }


# This function get's triggered by a cron job to syncronize on an interval
# This ensures that the data is up to date, without sacrificing performance on get requests from a consumer
def syncronisePokemonTeam():
    result = getPokemonTeam()

    # Insert the pokemon team object into DynamoDB
    dynamodb_client.put_item(
        TableName=TEAMS_TABLE,
        Item={
            'pk': {
                'S': 'team'
            },
            'sk': {
                'S': 'pokemon'
            },
            'team': {
                'M': result
            }
        }
    )

# This function get's triggered by a cron job to syncronize on an interval
# This ensures that the data is up to date, without sacrificing performance on get requests from a consumer
def syncroniseStarWarsTeam():
    result = getStarWarsTeam()

    # Insert the pokemon team object into DynamoDB
    dynamodb_client.put_item(
        TableName=TEAMS_TABLE,
        Item={
            'pk': {
                'S': 'team'
            },
            'sk': {
                'S': 'starwars'
            },
            'team': {
                'M': result
            }
        }
    )
