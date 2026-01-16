import json
import uuid
import boto3
from botocore.exceptions import BotoCoreError , ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TextAnalysis')

def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        text = body.get('text') or ''

        if not isinstance(text, str):
            raise ValueError('Invalid input: text must be a string')
        
        result = {
            'id' : str(uuid.uuid4()),
            'text' : text,
            'word_count' : len(text.split()),
            'char_count': len(text)
        }

        try:
            table.put_item(Item=result)

        except(BotoCoreError,ClientError)as db_err:
            print(f"DynamoDB error : {db_err}")
            return{
                'statusCode' : 500,
                'body':json.dumps({'error' : 'Failed to store data in Database.'})
            }
        
        return{
            'statusCode' : 200,
            'body' : json.dumps(result)
        }
    
    except json.JSONDecodeError:
        return{
            'statusCode' : 400,
            'body' : json.dumps({'error' : 'Invalid JSON format.'})
        }
    except ValueError as ve:
        return{
            'statusCode':400,
            'body' : json.dumps({'error' : str(ve)})
        }
    
    except Exception as e:
        return{
            'statusCode' : 500,
            'body' : json.dumps({'erroe' : 'Unexpected error occurred'})
        }