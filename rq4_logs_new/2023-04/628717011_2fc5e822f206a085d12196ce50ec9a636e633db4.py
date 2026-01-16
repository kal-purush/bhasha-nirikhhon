from dotenv import load_dotenv
import os

from flask import Flask, jsonify
from tgtg import TgtgClient
from tgtg.exceptions import TgtgAPIError, TgtgLoginError
from twilio.rest import Client

app = Flask(__name__)

# load environment variables from .env file
load_dotenv()

# Twilio configuration
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', None)
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', None)
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER', None)
MY_PHONE_NUMBER = os.getenv('MY_PHONE_NUMBER', None)
print(TWILIO_ACCOUNT_SID)
print(TWILIO_AUTH_TOKEN)
print(TWILIO_PHONE_NUMBER)
print(MY_PHONE_NUMBER)

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# TGTG client configuration
TGTG_ACCESS_TOKEN = os.getenv('TGTG_ACCESS_TOKEN')
TGTG_REFRESH_TOKEN = os.getenv('TGTG_REFRESH_TOKEN')
TGTG_USER_ID = os.getenv('TGTG_USER_ID')
TGTG_COOKIE = os.getenv('TGTG_COOKIE')
print(TGTG_ACCESS_TOKEN, "TG_ACCESS_TOKEN")
print(TGTG_REFRESH_TOKEN, "TG_REFRESH_TOKEN")
print(TGTG_USER_ID, "TG_USER_ID")
print(TGTG_COOKIE, "TG_COOKIE")

client = TgtgClient(
    access_token="e30.eyJzdWIiOiIyMTAwOTQ4NyIsImV4cCI6MTY4MTgzNjMyMCwidCI6IjRlNGN4RlNKUnBHOU5kZWM5aUQzblE6MDoxIn0.YESpTuEmBxFFcbsLjM0pG741kom3r0k7X2nnOpV6ocA",
    refresh_token="e30.eyJzdWIiOiIyMTAwOTQ4NyIsImV4cCI6MTcxMzI4NTkyMCwidCI6IkJSeHl6dGZqUzZhTTlXLXVoTmlPS1E6MDowIn0.uyJIopN44-0NzW3VqrFLZHuhe39tDlvYl5mJ4Vnca7E",
    user_id="21009487",
    cookie="datadome=2qjkq3g2XLtsWBe7zB71tB_2OmwS67trJ0tDNZi7T1LwN~s7Q8-4XM9-Z_bEy39Xxs1qwUsBhj-I4qsuB8Ntef_UG54AeyltQYAk4kbAKPb~XbhTQMV6-0Niu87m3Qrb"
)


def send_sms(message):
    try:
        message = twilio_client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=MY_PHONE_NUMBER
        )
        print(f"SMS sent: {message.sid}")
    except Exception as e:
        print(f"Error sending SMS: {e}")


def is_access_token_valid():
    try:
        client.get_items()
        return True
    except TgtgAPIError as e:
        if e.status_code == 401:  # Unauthorized
            return False
        else:
            raise e
    except Exception as e:
        print(f"Error checking access token: {e}")
        return False


@app.route('/')
def home():
    if not is_access_token_valid():
        send_sms("Access token is not valid. Please refresh the token.")
        return jsonify({"error": "Access token is not valid"}), 401

    try:
        items = client.get_items()
        for item in items:
            if item["items_available"] > 0:
                print('available')
                send_sms(f"{item['display_name']} is available! Go get it! :)")
            else:
                print('not available')
        return jsonify(items)
    except Exception as e:
        print(f"Error fetching items: {e}")
        return jsonify({"error": "Error fetching items"}), 500


if __name__ == '__main__':
    app.run(port=5001)