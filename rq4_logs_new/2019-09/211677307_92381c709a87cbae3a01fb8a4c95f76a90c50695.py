import json
from base64 import b64decode
from Cryptodome.Cipher import AES
key = b'X\xe9\xc2"+\x80\x86P$\x98\xa4\x84B\xecl\x1d\xf7<\x17f\xde\xb5\x97\x8e!\x94\xee8\x85\xfe\xebY'
# We assume that the key was securely shared beforehand
json_input = {"nonce": "pRp1LbWp9JU=", "ciphertext": "rJcmg3TN"}
str_json = json.dumps(json_input)
b64 = json.loads(str_json)
nonce = b64decode(b64['nonce'])
print(b64)
ct = b64decode(b64['ciphertext'])
cipher = AES.new(key, AES.MODE_CTR, nonce=nonce)
pt = cipher.decrypt(ct)
print("The message was: ", pt)