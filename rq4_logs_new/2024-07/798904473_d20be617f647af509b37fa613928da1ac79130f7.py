import requests
from bs4 import BeautifulSoup
from twilio.rest import Client
from twilio.http.http_client import TwilioHttpClient
import os


TWILIO_ACCOUNT_SID = ""
TWILIO_AUTH_TOKEN = ""


URL = "https://appbrewery.github.io/instant_pot/"


# Send GET request to the URL
response = requests.get(URL)
response.raise_for_status()  # Raise an exception for bad status codes
amazon_subpage = response.text

# Parse the HTML content of the page
soup = BeautifulSoup(amazon_subpage, "html.parser")


# Find current product price
pot_price_whole = soup.find(name="span", class_="a-price-whole").getText()
pot_price_fraction = soup.find(name="span", class_="a-price-fraction").getText()
pot_full_price = float(pot_price_whole + pot_price_fraction)


if pot_full_price < 100:
    proxy_client = TwilioHttpClient()
    proxy_client.session.proxies = {'https': os.environ['https_proxy']}

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, http_client=proxy_client)
    message = client.messages.create(
        body=f"God price for pot today: ${pot_full_price}. Let's buy it!",
        from_="+",
        to="+",
    )

    print(message.status)