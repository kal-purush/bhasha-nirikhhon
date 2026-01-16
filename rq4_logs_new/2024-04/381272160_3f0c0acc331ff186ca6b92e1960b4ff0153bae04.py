import json
import yfinance as yf
import os

try:
    stock_list_file = open("Data/config.json")
except:
    print("Add Stock names to the list !")
    exit()

stock_list = [stock['id'] for stock in json.load(stock_list_file)['stock_list']]

stock_tickers = [yf.Ticker(id) for id in stock_list]


os.makedirs("Data",exist_ok=True)
with open("Data/industry.csv",'w') as f:
    f.write("Name, Industry\n")
    for name,ticker in zip(stock_list,stock_tickers):
        f.write(f"{name},{ticker.info['industry']}\n")