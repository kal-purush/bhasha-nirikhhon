import jwt
from datetime import datetime, timedelta

# Define the payload for the token
payload = {
    'user_id': 123,
    'username': 'wolt',
    'exp': datetime.utcnow() + timedelta(hours=1)  # Expiration time (e.g., 1 hour from now)
}

# Define a secret key to sign the token
secret_key = 'ItIsWhatItIs'

# Generate the token
token = jwt.encode(payload, secret_key, algorithm='HS256')

print(token)