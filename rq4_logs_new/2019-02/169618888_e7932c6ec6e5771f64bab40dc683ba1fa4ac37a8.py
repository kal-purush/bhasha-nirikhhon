import requests
import json
import time
import datetime

#Creating all variables
API_TOKEN = "<Totango-Token>"
account_id = '0'
service_id = 'SP-<Totango-ServiceID>-01'
activity_type_id = '<Totango-Activity-Type>'
totango_user = '<Totango-User-Email>'

headers = {
    'app-token': API_TOKEN,
}


#Generating today's time in ISO 8601 and also epoch (per Totango's instructions)
date_iso = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.0Z")
now = datetime.datetime.now()
date_time = now.strftime("%m.%d.%Y %H:%M:%S")
pattern = '%m.%d.%Y %H:%M:%S'
epoch = int(time.mktime(time.strptime(date_time, pattern)))*1000


#Create a Segment in Totango, generate the API Endpoint and add the Query here
#Example below searches by account by a certain name and returns the Sales Manager and Success Manager
#Totango also always returns 'Last Activity' (if any), 'Account Name', and 'Account Number'
data = {
  'query': ' {"terms":[{"type":"string","term":"search_name","contains":"<ACCOUNT-NAME>"}],"count":1000,"offset":0,"fields":[{"type":"string_attribute","attribute":"Sales Manager","field_display_name":"<DEFINED_FIELD>"},{"type":"string_attribute","attribute":"Success Manager","field_display_name":"<DEFINED_FIELD>"}],"scope":"all"}'
}


#Requests all accounts based on serch query, decodes JSON and finds the number of hits (accounts under the specified Segment)
listed_accounts = requests.post('https://app.totango.com/api/v1/search/accounts', headers=headers, data=data)
listed_accounts_columns = json.loads(listed_accounts.content)
total_hits = listed_accounts_columns['response']['accounts']['total_hits']


#Once you have a list of accounts you could define the creation of a Touchpoint or update a specific Totango field
#Loop statement based on the total number of returned accounts
for row in range(total_hits):
    sales_manager = listed_accounts_columns['response']['accounts']['hits'][row]['selected_fields'][0]
    success_manager = listed_accounts_columns['response']['accounts']['hits'][row]['selected_fields'][1]
    account_id = listed_accounts_columns['response']['accounts']['hits'][row]['name']


    #Create a message to be added as a Touchpoint within an account
    message = "Test Message"

    data = {
        'account_id': account_id,
        'content': message,
        'activity_type_id': activity_type_id,
        'user_id': totango_user,
        'create_date': epoch
    }

    response = requests.post('https://app.totango.com/api/v3/touchpoints/', headers=headers, data=data)


    #Updates an account field value based on parameters below
    #Example below will update the Number of Licenses to 1
    params_account = (
        ('sdr_s', service_id),
        ('sdr_o', account_id),
        ('sdr_o.Licenses', 1)
    )

    response = requests.get('https://sdr.totango.com/pixel.gif/', params=params_account)



#Below is the Python Request when updating user information in Totango
#Very similar to the account update process, the only difference is:
#   1. You need the field sdr_u for the customer email
#   2. All user fields are 'sdr_u.'

    #params_users = (
    #    ('sdr_s', service_id),
    #    ('sdr_o', account_id),
    #    ('sdr_u', user_email),
    #    ('sdr_u.field_name', field),
    #)

    #response = requests.get('https://sdr.totango.com/pixel.gif/', params=params_users)