import requests
import os
import telebot
import telegram_send
import threading
import time
import traceback
import sys
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
alert_bot = telebot.TeleBot(API_KEY)

contract_addresses = [["https://io4.dexscreener.io/u/trading-history/recent/avalanche/0x4d308C46EA9f234ea515cC51F16fba776451cac8",100000], ["https://io4.dexscreener.io/u/trading-history/recent/avalanche/0x113f413371fc4cc4c9d6416cf1de9dfd7bf747df",100000]]#, ["https://io4.dexscreener.io/u/trading-history/recent/ethereum/0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",1000000]]
threadlist = []
pairs = []
tx_dict = {}
first_run = True

def get_data(contract_address, threshold):
    global first_run
    time.sleep(1)
    global API_KEY
    try:
        while True:
            r = requests.get(url = contract_address)
            trading_history = r.json()['tradingHistory']
            if first_run:
                for i in trading_history:
                    if float(i['volumeUsd'].replace(",","")) >= threshold:
                        if i['txnHash'] not in tx_dict:
                            tx_dict[i['txnHash']] = i
                first_run = False
                continue

            for i in trading_history:
                if float(i['volumeUsd'].replace(",","")) >= threshold:
                    if i['txnHash'] not in tx_dict:
                        tx_dict[i['txnHash']] = i
                        snowtrace_link = "https://snowtrace.io/tx/" + i['txnHash']
                        num_whale_emoji = int(float(i['volumeUsd'].replace(",","")))//50000
                        indicator = "🟢" if i['type'] == "buy" else "🔴"
                        response = f"{num_whale_emoji * '🐳'}\n\n${r.json()['baseTokenSymbol']}/{r.json()['quoteTokenSymbol']}\n\n{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(i['blockTimestamp']/1000))}\n\n {indicator} ${i['volumeUsd']} {i['type']} @ ${i['priceUsd']}\n\nTxn Hash: {i['txnHash']}"
                        print(response)
                        print(API_KEY)
                        requests.get(f'https://api.telegram.org/bot{API_KEY}/sendMessage?chat_id=-1001669583938&text={response}&disable_web_page_preview=true')


    except:
        print(traceback.format_exc())


def main():
    global contract_addresses
    global threadlist
    for i in contract_addresses:
        t = threading.Thread(target=get_data,args=tuple([i[0],i[1]]))
        t.start()
        threadlist.append(t)
    for thread in threadlist:
        thread.join()
    alert_bot.polling()


if __name__ == "__main__":
    while True:
        main()