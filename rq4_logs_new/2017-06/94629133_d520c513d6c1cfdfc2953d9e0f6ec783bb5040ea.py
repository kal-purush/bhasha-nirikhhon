# -*- coding: utf-8 -*-
import requests
import datetime


def format_currency(value):
    # return "₩{:,.2f}".format(int(value))
    return "₩{:,}".format(int(value))

class CoinoneAPI:

    @staticmethod
    def getKeyForSort(order):
        return order['timestamp']

    def get_last_order_info(self, currency="btc"):
        return self.recentOrders(currency)

    def recentOrders(self, currency="btc"):
        url = "https://api.coinone.co.kr/trades/"
        params = {'currency': currency}
        res = requests.get(url, params=params)
        if res.status_code != 200:
            return ""
        orders = res.json()['completeOrders']
        sorted(orders, key=CoinoneAPI.getKeyForSort)
        return orders[-1]


class SlackResponder:

    @staticmethod
    def currency(currency):
        order_info = CoinoneAPI().get_last_order_info(currency)
        return SlackResponder().create_attachments(currency, order_info)

    def create_attachments(self, currency, order_info):
        time = datetime.datetime.fromtimestamp(
            int(order_info['timestamp'])
        ).strftime('*%Y-%m-%d %H:%M:%S*+09:00')
        attachments = [{
            "color": "#36a64f",
            "author_name": "CoinChecker",
            "author_icon": ("https://cdn3.iconfinder.com/data/icons"
                            "/payment-method-1/64/_bitcoin-128.png"),
            "title": "Virtual Coin currency",
            "title_link": "https://github.com/9to6",
            "text": "The time that last ordered is at %s" % time,
            "mrkdwn_in": ["text"],
            "fields": [
                {
                    "title": currency,
                    "value": format_currency(order_info['price']),
                    "short": "true"
                }
            ]
        }]
        return attachments

# api = CoinoneAPI()
# order_info = api.get_last_order_info("xrp")
# print SlackResponder().create_attachments("xrp", order_info)
# print order_info