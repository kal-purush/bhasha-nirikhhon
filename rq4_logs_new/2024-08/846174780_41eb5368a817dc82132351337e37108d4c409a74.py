# from alpaca.data.historical import CryptoHistoricalDataClient
# from alpaca.data.requests import CryptoBarsRequest
# from alpaca.data.timeframe import TimeFrame

# # no keys required for crypto data
# client = CryptoHistoricalDataClient()

# request_params = CryptoBarsRequest(
#                         symbol_or_symbols=["BTC/USD", "ETH/USD"],
#                         timeframe=TimeFrame.Day,
#                         start="2022-07-01"
#                  )

# bars = client.get_crypto_bars(request_params)
# print(bars)
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_API_SECRET")

def stream_ticker(symbol: str) -> None:
    from alpaca.data.live.stock import StockDataStream

    stock_data_stream_client = StockDataStream(api_key, secret_key, url_override = None)

    async def stock_data_stream_handler(data):
        print(data)

    symbols = [symbol]

    stock_data_stream_client.subscribe_quotes(stock_data_stream_handler, *symbols) 
    stock_data_stream_client.subscribe_trades(stock_data_stream_handler, *symbols)

    stock_data_stream_client.run()

if __name__ == "__main__":
    
    symbol = "AAPL"

    from alpaca.data.historical.stock import *
    from datetime import datetime
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    # setup stock historical data client
    stock_historical_data_client = StockHistoricalDataClient(api_key, secret_key, url_override = None)

    # get historical bars by symbol
    now = datetime.now(ZoneInfo("America/New_York"))
    req = StockBarsRequest(
        symbol_or_symbols = [symbol],
        timeframe=TimeFrame(amount = 1, unit = TimeFrameUnit.Hour), # specify timeframe
        start = now - timedelta(days = 10),                          # specify start datetime, default=the beginning of the current day.
        end_date=None,                                        # specify end datetime, default=now
        limit = 20,                                               # specify limit
    )
    df = stock_historical_data_client.get_stock_bars(req).df
    print(df)
        
    # stream_ticker(symbol=symbol)