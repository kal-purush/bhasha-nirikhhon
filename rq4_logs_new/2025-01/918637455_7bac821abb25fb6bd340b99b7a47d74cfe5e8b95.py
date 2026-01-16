import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import json
import os

# Nome do bucket e arquivo S3
BUCKET_NAME = os.environ.get("BUCKET_NAME", "nome-do-seu-bucket")
FILE_NAME = "last_execution.json"

# Cliente S3
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Tentar obter o arquivo do bucket
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
        file_content = response['Body'].read().decode('utf-8')
        last_execution = json.loads(file_content).get("last_execution")
    except ClientError as e:
        # Se o arquivo não existir, criar um novo
        if e.response['Error']['Code'] == 'NoSuchKey':
            last_execution = None
        else:
            raise e

    # Registrar a data e hora da execução atual
    current_execution = datetime.utcnow().isoformat()
    new_data = {
        "last_execution": current_execution
    }

    # Atualizar o arquivo no S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=FILE_NAME,
        Body=json.dumps(new_data),
        ContentType='application/json'
    )

    # Responder com a data da última execução ou mensagem inicial
    if last_execution:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Lambda executed successfully.",
                "last_execution": last_execution,
                "current_execution": current_execution
            })
        }
    else:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Lambda executed for the first time.",
                "current_execution": current_execution
            })
        }