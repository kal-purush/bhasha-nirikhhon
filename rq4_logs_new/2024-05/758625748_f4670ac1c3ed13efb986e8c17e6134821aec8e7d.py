from binance.spot import Spot

#Creating the client

def logging():

    client = Spot(api_key='Lv5JJuzSqlkzDIStlc4BmZSSQsPzYgg3mTdkRwjyHaGnmLR5bvizK6bNGIDOt7pf',
                  api_secret='NykICJXcr9mydvUWaPxA8Krd9wakHHnLAA19VGmmjda3RtzgWE6rgadQFhU03HeY')

    return client

#Make a limit order

def make_order(order, price):

    client = logging()

    order_limit = {'symbol':'BTCUSDT',
               'side': order,
               'type': 'LIMIT',
               'price': price,
               'quantity': '0.0001',
               'timeInForce': 'GTC'}

    client.new_order(**order_limit)

#close, high, low = 62684.58,62719.29,62684.57

#execute_order(close, high, low)