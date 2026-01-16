# alerts.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import config
import requests

def fetch_stock_price(symbol):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval': '1min',
        'apikey': config.ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    latest_close = list(data['Time Series (1min)'].values())[0]['4. close']
    return float(latest_close)

def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = config.EMAIL_ADDRESS
    msg['To'] = config.EMAIL_ADDRESS
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
    text = msg.as_string()
    server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_ADDRESS, text)
    server.quit()

def check_alerts():
    symbol = 'AAPL'
    price_threshold = 150  # Example threshold
    current_price = fetch_stock_price(symbol)
    
    if current_price > price_threshold:
        subject = f'Alert: {symbol} price above {price_threshold}'
        body = f'The current price of {symbol} is {current_price}, which is above your threshold of {price_threshold}.'
        send_email(subject, body)

if __name__ == "__main__":
    check_alerts()