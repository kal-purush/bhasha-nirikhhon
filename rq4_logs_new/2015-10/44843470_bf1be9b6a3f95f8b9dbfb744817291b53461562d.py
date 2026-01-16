import json
import hmac
from hashlib import sha256
from base64 import urlsafe_b64encode, urlsafe_b64decode
import sys


token = sys.argv[1]

signing_input, signature = token.rsplit('.', 1)
header, payload = signing_input.rsplit('.', 1)

header_data = urlsafe_b64decode(header)

#payload_data = urlsafe_b64decode(payload)
payload += '='
payload_data = urlsafe_b64decode(payload)


print 'header_data', header_data
print 'payload_data', payload_data
