
from binance.client import Client
import pandas as pd
import time
import pickle

api_key = 'your_api_key'
api_secret = 'your_api_secret'

# https://python-binance-jp.readthedocs.io/ja/latest/
client = Client(api_key, api_secret)

symbol = 'BTCUSDT'
# https://python-binance-jp.readthedocs.io/ja/latest/enums.html
interval = Client.KLINE_INTERVAL_1MINUTE
limit = 500
start_date = '2022-01-01'
end_date = '2023-02-18'
start_time = int(pd.Timestamp(start_date).timestamp() * 1000)
end_time = int(pd.Timestamp(end_date).timestamp() * 1000)
df = pd.DataFrame()

while start_time < end_time:
    candles = client.get_klines(symbol=symbol, interval=interval, limit=limit, startTime=start_time)
    df = df.append(pd.DataFrame(candles, \
                                columns= [
                                    'timestamp',
                                    'op',
                                    'hi',
                                    'lo',
                                    'cl',
                                    'volume',
                                    'close_time',
                                    'quote_asset_volume',
                                    'number_of_trades',
                                    'taker_buy_base_asset_volume',
                                    'taker_buy_quote_asset_volume',
                                    'ignore'
                                ]))
    start_time = int(candles[-1][6]) + 1
    time.sleep(0.5) # APIリクエストの過負荷を避けるために、リクエスト間隔を0.5秒に設定する

df = df[['timestamp', 'op', 'hi', 'lo', 'cl', 'volume']] # カラムを修正する
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
print(df)

# ループをbreakで抜けた後に、CSVとPickle形式でデータフレームを保存する
if not df.empty:
    df.to_csv('df_ohlcv.csv', index=False)
    with open('df_ohlcv.pkl', 'wb') as f:
        pickle.dump(df, f)

print(df)