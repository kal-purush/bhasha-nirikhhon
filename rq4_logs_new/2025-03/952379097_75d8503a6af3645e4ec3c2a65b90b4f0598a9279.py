import requests

def get_access_token():
    consumer_key ="Your Consumer Key"
    consumer_secret = "YourConsumer Secret"
    api_url = "https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials"

    try:
        response = requests.get(api_url, auth=(consumer_key, consumer_secret))
        print("Raw Response:", response.text)  # Log response for debugging

        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Network error: {e}")

def stk_push(phone_number, amount):
    access_token = get_access_token()
    api_url = "https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/processrequest"
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {
        "BusinessShortCode": "code",
        "Password": "Password",
        "Timestamp": "20160216165627",
        "TransactionType":"CustomerPayBillOnline",
        "Amount": 1,
        "PartyA": Mpesa_Number,
        "PartyB": "174379",
        "PhoneNumber": Mpesa_Number,
        "CallBackURL": "https://mydomain.com/pat",
        "AccountReference": "Test",
        "TransactionDesc": "Test",
    }

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        print("STK Push Response:", response.text)
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Network error: {e}")
    except ValueError:
        raise Exception("Failed to parse JSON response. Raw response: " + response.text)

# Example usage
try:
    print(stk_push("Mpesa_Number", 10))
except Exception as e:
    print("Error:", e)