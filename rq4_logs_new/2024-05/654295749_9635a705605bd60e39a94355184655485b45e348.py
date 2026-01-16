import mplfinance as mpf
from datetime import datetime
import numpy as np
import pandas as pd
from pybit.unified_trading import HTTP


class BybitDownloader:
    def __init__(self, symbol, period, start_date=None, init_data=None):
        self.symbol = symbol
        self.period = period
        self.start_date = start_date
        self.init_data = init_data
        
    def get_klines(self, start_date, size: int=1000, end: datetime=None): 
        if start_date is not None and isinstance(start_date, datetime):
            start_date=start_date.timestamp()*1000
        if end is not None and isinstance(end, datetime):
            end=end.timestamp()*1000

        session = HTTP()
        message = session.get_kline(
            category="linear",
            symbol=self.symbol,
            interval=str(self.period),
            start=start_date,
            end=end,
            limit=size
        )
        return message["result"]

    @staticmethod
    def _get_data_from_message(mresult):
        data = {}
        input = np.array(mresult["list"], dtype=np.float64)[::-1]
        data["Date"] = pd.to_datetime(input[:, 0].astype(np.int64)*1000000)
        data["Open"] = input[:, 1]
        data["High"] = input[:, 2]
        data["Low"]  = input[:, 3]
        data["Close"]= input[:, 4]
        data["Volume"] = input[:, 5]
        data = pd.DataFrame(data)
        data.set_index(data.Date, drop=True, inplace=True)
        data.drop("Date", axis=1, inplace=True)
        return data

    def read_from_file(self, filename):
        # filename - csv file name
        data = pd.read_csv(filename)
        data.Date = pd.to_datetime(data.Date, format="mixed")
        data.set_index(data.Date, drop=True, inplace=True)
        data.drop("Date", axis=1, inplace=True)
        return data
                

    def get_history(self):
        if self.init_data is not None:
            h = self.read_from_file(self.init_data)
            self.start_date = h.index[-1]
        else:
            h = self._get_data_from_message(self.get_klines(start_date=self.start_date))
        print(h.index[-1])
        res = self._get_data_from_message(self.get_klines(start_date=h.index[-1]))[1:]
        h = pd.concat([h, res])
        while res.shape[0]:
            print(h.index[-1])
            res = self._get_data_from_message(self.get_klines(start_date=h.index[-1]))[1:]
            h = pd.concat([h, res])
        return h


if __name__ == "__main__":
    symbol = "ETHUSDT"
    period = 15
    init_data = f"data/bybit/M{period}/{symbol}_M{period}.csv"
    bb_loader = BybitDownloader(symbol=symbol, 
                                period=period, 
                                start_date=None,
                                init_data=init_data)
    
    h = bb_loader.get_history()
    h.to_csv(init_data)
    
    print(h)
    mpf.plot(h, type='line')