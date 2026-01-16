#!/usr/bin/env python3
from time import time, sleep
import robin_stocks
import robin_stocks.robinhood as r

userName = input("Please enter your username:\n")
passWord = input("Please enter your password:\n")

import robin_stocks.robinhood as r
login = r.login(userName, passWord)

# End of Authentication and Initiation Area
# --------------------------------------------------------------------

while True:
	sleep(1 - time() % 1)
	my_stocks = r.build_holdings()
	for key,value in my_stocks.items():
		print(key,value)