import requests
import json
import sys

#Get EcoBee application PIN and tokens

#Reads developer key from a file

a=open("apikey.txt","r")
APP_KEY = a.read()

#Define scope

SCOPE = 'smartWrite'

#Get PIN

auth_url = 'https://api.ecobee.com/authorize?'
payload = {'response_type':'ecobeePin','client_id': APP_KEY,'scope' : SCOPE}

r = requests.get(auth_url,payload)

r_dict = json.loads(r.text)

print('Your PIN is: ',r_dict['ecobeePin'])

user_resp = input('Type Y once app has been authorized: ')

if user_resp == "Y":

    # get tokens

    auth_code = r_dict['code']
    token_url = 'https://api.ecobee.com/token?'
    payload = {'grant_type': 'ecobeePin', 'code': auth_code, 'client_id': APP_KEY}

    try:

        r_tokens = requests.post(token_url, params=payload)
        r_tokens_dict = json.loads(r_tokens.text)

    except:

        print("Authorization Error - Try Again")

    access_token = r_tokens_dict['access_token']
    token_type = r_tokens_dict['token_type']
    refresh_token = r_tokens_dict['refresh_token']

    auth_log = access_token + "," + token_type + "," + refresh_token

    # Write tokens to file

    f = open("tokens.txt", "w+")
    f.truncate()
    f.write(auth_log)
    f.close()

    fb = open("tokensbackup.txt", "w+")
    fb.truncate()
    fb.write(auth_log)
    fb.close()

    # verify tokens are good

    f = open("tokens.txt", "r")
    token_str = str(f.read())
    f.close()

    print('Good tokens! ', token_str)