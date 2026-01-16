import oauth2
import json

# Generate your key and secret by running  python manage.py create_client
consumer_key = "... key ..."
consumer_secret = "... secret ..."

registration_server = "http://... your host ..."

consumer = oauth2.Consumer(key=consumer_key, secret=consumer_secret)
client = oauth2.Client(consumer)

data = {
    "name": "Command Line Demo",
    "prereqs": ["sws-dev", "pws-dev", "made-up-dev"],
    "login_systems": ["Shibboleth", "Social Gateway"],
    "log_services": ["test-splunk"],
    "hosts": [
        {"name": "ts-app01", "role": "application"},
        {"name": "ts-app02", "role": "application"},
        {"name": "ts-app03", "role": "application"},
        {"name": "ts-app04", "role": "application"},
        {"name": "ts-db01", "role": "database"},
    ]
}


# Create a service
client.request("%s/api/v1/service" % (registration_server),
               method='POST',
               body=json.dumps(data),
               headers={"Content-Type": "application/json"})