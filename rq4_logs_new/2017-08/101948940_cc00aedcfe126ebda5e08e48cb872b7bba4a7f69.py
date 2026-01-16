# -*- coding: utf-8 -*-
"""
Bittrex-module by Caio Vivas

"""

import json
import urllib.request
from bittrex import bittrex

class Py3status:
	
	format_a = "{cursymbol} {fiat_bal:.2f}"
	format_b = "{cursymbol} {btc_bal:.4f}"
	format_c = "{cursymbol}Bittrex"
	upsymbol = "+"
	downsymbol = "-"
	equalsymbol = "="
	api_key = ""
	api_secret = ""	
	fiat = "USD"
	cache_timeout = 60

	def __init__(self):
		self.show_btc = 1
		self.fiat_bal = 0
		self.last_btc = 0
		self.btc_bal = 0
		self.cursymbol = "="
		self.api_bt = None	

	def bittrex_module(self):
		if(self.api_bt == None):
			self.api_bt = bittrex.Bittrex(self.api_key, self.api_secret)
		self.last_btc = self.btc_bal
		self.btc_bal = self.__get_total_btc()
		
		if(self.btc_bal > self.last_btc):
			self.cursymbol = self.upsymbol
			color = self.py3.COLOR_GOOD
		elif(self.btc_bal == self.last_btc):
			self.cursymbol = self.equalsymbol
			color = self.py3.COLOR_DEGRADED
		else:
			self.cursymbol = self.downsymbol
			color = self.py3.COLOR_BAD

		self.fiat_bal = self.__get_total_fiat(self.__get_btc_usd(), self.btc_bal)
		data = {"fiat_bal" : self.fiat_bal, "btc_bal" : self.btc_bal, "show_mode" : self.show_btc, "cursymbol" : self.cursymbol}
		full_text = ""
		if(self.show_btc == 1):
			full_text = self.py3.safe_format(self.format_a, data)
		elif(self.show_btc == 2):
			full_text = self.py3.safe_format(self.format_b, data)
		elif(self.show_btc == 3):
			full_text = self.py3.safe_format(self.format_c, data)
		return{
			"full_text" : full_text,
			"color" : color,
			"cached_until" : self.py3.time_in(self.cache_timeout)
		}
	
	def on_click(self, event):
		if(event["button"] == 1):
			if(self.show_btc == 3):
				self.show_btc = 1
			else:
				self.show_btc += 1

	def __get_fiat(self):
		if(self.fiat == "USD"):
			return None
		with urllib.request.urlopen("http://api.fixer.io/latest?base=USD") as url:
			data = json.loads(url.read().decode())
			return float(data["rates"][self.fiat])
	
	def __get_btc_usd(self):
		with urllib.request.urlopen("https://api.coinmarketcap.com/v1/ticker/bitcoin/") as url:
			data = json.loads(url.read().decode())
			return float(data[0]["price_usd"])

	def __get_total_btc(self):
		current_btc = 0
		wallet_data = self.api_bt.get_balances()
		market_data = self.api_bt.get_market_summaries()
		for c in wallet_data["result"]:
			if(float(c["Balance"]) <= 0):
				continue
			value = 0.0
			for m in market_data["result"]:
				if(m["MarketName"] == ("BTC-"+c["Currency"])):
					value = float(m["Last"])
			current_btc += value * float(c["Balance"])
		return float(current_btc)
	
	def __get_total_fiat(self, price_usd, total_btc):
		usd_value = price_usd * total_btc
		if(self.fiat == "USD"):
			return usd_value
		return self.__get_fiat() * usd_value
