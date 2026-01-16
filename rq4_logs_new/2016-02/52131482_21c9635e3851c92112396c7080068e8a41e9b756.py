"""

Basic example that prints data to the console.

"""

from stock_watcher.yahoo_quotes import (
    PRICE_QUOTE_FIELDS,
    FUNDAMENTAL_QUOTE_FILEDS,
    yahoo_quotes
)

symbols = ['WMT', 'JPM', 'XOM', 'AMZN', 'CMG']

prices = yahoo_quotes(symbols, PRICE_QUOTE_FIELDS)
fundamentals = yahoo_quotes(symbols, FUNDAMENTAL_QUOTE_FILEDS)


print "\nDownloading data for %s\n" % symbols

print "Price Quotes"
for k, v in prices.items():
    print k, v

print
print "Company Fundamentals"
for k, v in fundamentals.items():
    print k, v