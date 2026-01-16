import conjur

# configure conjur connection details
conjur.configure(appliance_url='https://conjur',
		account='PwC',
		cert_path='/home/admin/conjur.pem')
# For God's sake, don't put passwords in your source code!
password = ''
login = ''

# Create an API instance that can perform actions as the user 'alice'
api = conjur.new_from_key(login, password)

# Use the API to fetch the value of a variable

secret = api.variable('db_password').value()

print("The secret is '{}'".format(secret))