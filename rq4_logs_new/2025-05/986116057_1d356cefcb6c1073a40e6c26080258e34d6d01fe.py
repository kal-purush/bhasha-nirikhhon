import json
import boto3
import os
import base64
import time
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet

# AWS Clients
secrets_client = boto3.client('secretsmanager')
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Config
SECRET_NAME = os.environ.get('SECRET_NAME', 'doclockbox/encryption-key')
SHARING_TABLE = os.environ.get('SHARING_TABLE', 'sharing')
VERIFY_TABLE = os.environ.get('VERIFY_TABLE', 'verify_token')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'doclockbox-secure-uploads')

# Standard CORS headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': 'https://doclockbox.app',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
}

# Fetch encryption key once at cold start
def get_encryption_key():
    secret_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(secret_response['SecretString'])
    return secret['ENCRYPTION_KEY']

ENCRYPTION_KEY = get_encryption_key()
fernet = Fernet(ENCRYPTION_KEY)
sharing_table = dynamodb.Table(SHARING_TABLE)
verify_table = dynamodb.Table(VERIFY_TABLE)

def lambda_handler(event, context):
    try:
        # Get query parameters
        query_params = event.get('queryStringParameters', {})
        token = query_params.get('token')
        otp_input = query_params.get('otp')

        # body = json.loads(event.get('body', '{}'))
        # print(f"Received body: {body}")
        # token = body.get('token')
        # otp_input = body.get('otp')

        print(f"Received token: {token} and otp: {otp_input}")
        if not token or not otp_input:
            return {
                'statusCode': 400,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Missing token or otp parameter'})
            }

        # Decrypt token
        decrypted_payload = fernet.decrypt(token.encode('utf-8'))
        payload = json.loads(decrypted_payload.decode('utf-8'))

        email = payload.get('email')
        share_id = payload.get('share_id')
        key_b64 = payload.get('key')
        iv_b64 = payload.get('iv')
        filename = payload.get('filename')

        if not all([email, share_id, key_b64, iv_b64, filename]):
            return {
                'statusCode': 400,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Missing required values in token'})
            }

        print(f"Received email: {email} and share_id: {share_id}")
        # Step 1: Check OTP + expiration in verify_token table
        verify_resp = verify_table.get_item(Key={'share_id': share_id})
        verify_item = verify_resp.get('Item')

        print(f"Received verify_item: {verify_item}")

        if not verify_item:
            return {
                'statusCode': 404,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'OTP verification record not found'})
            }

        current_time = int(time.time())
        ttl = int(verify_item.get('ttl', 0))

        if current_time > ttl:
            return {
                'statusCode': 410,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'OTP has expired'})
            }

        stored_otp = verify_item.get('otp')
        if otp_input != stored_otp:
            print(f"OTP mismatch: {otp_input} != {stored_otp}")
            return {
                'statusCode': 401,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Invalid OTP'})
            }
        print(f"OTP matched, in db: {otp_input} and got: {stored_otp}")
        # Step 2: Decode key and iv
        dek = base64.b64decode(key_b64)
        iv = base64.b64decode(iv_b64)

        # Step 3: Fetch sharing record
        response = sharing_table.get_item(Key={'share_id': share_id, 'email': email})
        item = response.get('Item')

        if not item:
            return {
                'statusCode': 404,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'No sharing record found for token'})
            }

        # Check expiration date (if present)
        expiration_date = item.get('expiration_date')
        if expiration_date:
            now = datetime.now(timezone.utc)
            expiration_dt = datetime.fromisoformat(expiration_date.replace('Z', '+00:00'))
            if now > expiration_dt:
                return {
                    'statusCode': 410,
                    'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                    'body': json.dumps({'error': 'Download link has expired'})
                }

        # Check download limit
        download_limit = int(item.get('download_limit', 0))
        download_attempt = int(item.get('download_attempt', 0))
        if download_limit > 0 and download_attempt >= download_limit:
            return {
                'statusCode': 429,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Download limit reached'})
            }

        # Step 4: Fetch encrypted file from S3
        s3_path = item.get('s3_path')
        if not s3_path:
            return {
                'statusCode': 404,
                'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
                'body': json.dumps({'error': 's3_path not found in record'})
            }

        s3_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_path)
        encrypted_blob = s3_obj['Body'].read()

        # Assume last 16 bytes are GCM tag
        ciphertext = encrypted_blob[:-16]
        tag = encrypted_blob[-16:]

        # Decrypt file content
        cipher = Cipher(algorithms.AES(dek), modes.GCM(iv, tag), backend=default_backend())
        decryptor = cipher.decryptor()
        plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        # Update download attempt count
        sharing_table.update_item(
            Key={'share_id': share_id, 'email': email},
            UpdateExpression="""
                SET 
                    download_attempt = if_not_exists(download_attempt, :zero) + :inc,
                    download_status = :status
            """,
            ExpressionAttributeValues={
                ':inc': 1,
                ':zero': 0,
                ':status': 'DOWNLOADED'
            }
        )

        print(f"Download attempt updated for share_id: {share_id} and returnning the result: {base64.b64encode(plaintext).decode('utf-8')}")
        return {
            'statusCode': 200,
            'headers': {
                **CORS_HEADERS,
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': f'attachment; filename="{filename}"'
            },
            'isBase64Encoded': True,
            'body': base64.b64encode(plaintext).decode('utf-8')
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {**CORS_HEADERS, 'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error', 'details': str(e)})
        }