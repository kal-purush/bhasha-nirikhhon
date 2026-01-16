import krakenex
import re
import sys
import time
import datetime
import urllib
from urllib import request
import json

import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from email.mime.text import MIMEText
import base64
from send_email import send_email, create_message

login = os.getlogin()

def get_json(url):
  req = urllib.request.Request(
    url=url, 
    headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36'})
  response = urllib.request.urlopen(req)
  return json.loads(response.read().decode('utf-8'))

# Kraken

k = krakenex.API()
k.load_key('/Users/{}/keys/kraken.key'.format(login))

def check_kraken(coin):
  time.sleep(1)
  final = {
    'buy_variable_fee': 0.16/100,
    'currency': coin
  }
  if coin == 'BTC':
    final['transfer_fee'] = 0.001
    pair = "XBTEUR"
    pair2 = "XXBTZEUR"

  if coin == 'ETH':
    final['transfer_fee'] = 0.005

  if coin == 'LTC':
    final['transfer_fee'] = 0.02
    pair = "LTCEUR"
    pair2 = "XLTCZEUR"

  if coin == 'BCH':
    final['transfer_fee'] = 0.001
    pair = "BCHEUR"
    pair2 = "BCHEUR"

  print(pair)
  data = k.query_public('Ticker',{'pair': pair,})
  data = data['result']
  kraken = {}
  kraken['price_to_buy'] = data[pair2]['a'][0] # input('Kraken {} Price: '.format(coin)) 
  kraken['price_to_sell'] = 0 #data[pair2]['b'][0]
  final['price'] = float(kraken['price_to_buy'])
  final['date'] = datetime.datetime.now()
  return final

# Foxbit

def check_foxbit(coin):
  final = {
    'withdrawal_variable_fee': 1.39/100,
    'withdrawal_fixed_fee': 9.0,
    'sell_fee': 0.5/100,
    'currency': coin
  }
  if coin == 'BTC':
    final['transfer_fee'] = 0.001
  if coin == 'ETH':
    final['transfer_fee'] = 0.005
  if coin == 'LTC':
    final['transfer_fee'] = 0.02
  if coin == 'BCH':
    final['transfer_fee'] = 0.001
  url = "https://api.bitvalor.com/v1/ticker.json"
  data = get_json(url)
  timestamp = data['timestamp']['exchanges']['FOX']
  final['price'] = data['ticker_1h']['exchanges']['FOX']['last']
  final['date'] = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
  return final

# Official rates

def check_official(coin):
  final = {}
  data = get_json("http://api.fixer.io/latest")
  final['price'] = float(data['rates'][coin])
  return final


# Mercado Bitcoin

def check_mercado(coin):
  final = {
    'sell_fee': 0.7/100,
    'withdrawal_fixed_fee': 2.9,
    'withdrawal_variable_fee': 1.99/100,
    'currency': coin
  }
  url = ("https://www.mercadobitcoin.net/api/%s/ticker/" % coin)
  data = get_json(url)
  final['price'] = float(data['ticker']['last'])
  timestamp = data['ticker']['date']
  final['date'] = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
  return final

def compute_profit(exchange1, exchange2, official_rate):
  brl_to_sell = 10000.0
  itau_transfer_fee = 8.5
  withdrawal_fixed_fee = exchange2['withdrawal_fixed_fee']
  withdrawal_variable_fee = exchange2['withdrawal_variable_fee']
  sell_fee = exchange2['sell_fee']
  transfer_fee = exchange1['transfer_fee']
  buy_variable_fee = exchange1['buy_variable_fee']
  brl_to_buy = brl_to_sell / (1.0 - withdrawal_variable_fee) +  withdrawal_fixed_fee + itau_transfer_fee
  btc_to_sell = brl_to_buy * (1.0 + sell_fee) / exchange2['price']
  btc_to_buy = btc_to_sell + transfer_fee
  eur_to_sell = btc_to_buy * exchange1['price'] * (1.0 + buy_variable_fee)
  eur_to_ask = brl_to_sell / official_rate['price']
  profit = eur_to_ask - eur_to_sell
  delta = eur_to_ask / eur_to_sell - 1.0
  text_to_return = '%s: \nProfit selling 10k BRL: %.2f EUR \nDelta exchange rates: %.2f %% \n' % (exchange1['currency'], profit, delta*100)
  text_to_return += ('Buy %.2f %s for %.2f EUR\n' % (btc_to_buy, exchange2['currency'], eur_to_sell))
  text_to_return += ('EUR/%s %.2f, BRL/%s %.2f\n' % (exchange2['currency'], exchange1['price'], exchange2['currency'], exchange2['price']))
  text_to_return += ('BRL/%s/EUR: %.2f, BRL/EUR: %.2f\n\n' % (exchange2['currency'], exchange2['price']/exchange1['price'], official_rate['price']))
  return text_to_return

body = 'Foxbit: \n'
#print(check_kraken('BTC'))
#print(check_foxbit('BTC'))
#print(check_official('BRL'))
krake_btc = check_kraken('BTC')
official_brl = check_official('BRL')
body += compute_profit(krake_btc, check_foxbit('BTC'), official_brl)

body += 'Mercado Bitcoin: \n'
body += compute_profit(krake_btc, check_mercado('BTC'), official_brl)
body += compute_profit(check_kraken('LTC'), check_mercado('LTC'), official_brl)
body += compute_profit(check_kraken('BCH'), check_mercado('BCH'), official_brl)

print(body)

settings_crypto = json.load(open('/Users/{}/keys/settings_crypto.json'.format(login)))
emails = settings_crypto['emails']

should_send = input("Should I send an email? ")
if should_send == 'y':
	message = create_message('Crypto Trader', ','.join(emails), 'Alert delta BRL', body)
	send_email(message)


#kraken_eur_btc = check_kraken('BTC')['price']

# foxbit = check_foxbit('BTC')
# foxbit_brl_btc = foxbit['price']
# date_brl_btc = foxbit['date']

# brl_eur = check_official('BRL')['price']

# brl_btc_eur = float(foxbit_brl_btc)/float(kraken_eur_btc)

# # Compare rates
# delta = float((brl_btc_eur/brl_eur - 1) * 100.0)

# # Compute profit taking fees into consideration
# brl_to_sell = 10000.0
# foxbit_withdrawal_fixed_fee = 9.0
# foxbit_withdrawal_variable_fee = 1.39/100
# foxbit_sell_fee = 0.5/100
# kraken_transfer_fee = 0.001
# kraken_buy_fee = 0.16/100
# itau_transfer_fee = 8.5
# brl_to_buy = brl_to_sell / (1.0 - foxbit_withdrawal_variable_fee) +  foxbit_withdrawal_fixed_fee + itau_transfer_fee
# btc_to_sell = brl_to_buy * (1.0 + foxbit_sell_fee) / foxbit_brl_btc
# btc_to_buy = btc_to_sell + kraken_transfer_fee
# eur_to_sell = btc_to_buy * kraken_eur_btc * (1.0 + kraken_buy_fee)
# eur_to_ask = brl_to_sell / brl_eur

# profit = eur_to_ask - eur_to_sell
# effective_delta = (eur_to_ask/eur_to_sell  - 1) * 100

# Send email
# body = 'Time to buy BTC:'
# body += '\n\nKraken:  %.2f EUR/BTC' % kraken_eur_btc
# body += '\n' + 'Foxbit: %.2f BRL/BTC (%s)' % (foxbit_brl_btc, date_brl_btc)
# body += '\n' + 'BTC rate: %.2f BRL/EUR' % brl_btc_eur
# body += '\n' + 'Official exchange rate: %.2f BRL/EUR' % brl_eur
# body += '\n' + 'Delta: %.2f %%' % delta
# body += '\n\n' + 'Buy: %.2f BTC for %.2f EUR' % (btc_to_buy, eur_to_sell)
# body += '\n' + 'Sell: %.0f BRL for %.2f EUR' % (brl_to_sell, eur_to_ask)
# body += '\n' + 'Effective delta: %.2f %%' % effective_delta
# body += '\n' + 'Profit made: %.2f EUR' % profit

# print(body)