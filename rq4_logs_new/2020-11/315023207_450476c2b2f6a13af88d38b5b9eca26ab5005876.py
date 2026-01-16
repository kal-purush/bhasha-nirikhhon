from quotation_api import *
from kakao import *

def get_target_price(ticker):
  df = get_ohlcv(ticker)
  yesterday = df.iloc[-2]

  today_open = yesterday['close']
  yesterday_high = yesterday['high']
  yesterday_low = yesterday['low']
  target = today_open + (yesterday_high - yesterday_low) * 0.5
  return target

def get_yesterday_ma(ticker, roll):
  df = get_ohlcv(ticker)
  close = df['close']
  ma = close.rolling(roll).mean()
  return ma[-2]

def get_bull_market(tickers):
  markets = []
  for ticker, korean_name in tickers:
    yesteday_ma5 = get_yesterday_ma(ticker, 5)
    target_price = get_target_price(ticker)
    current_price = get_current_price(ticker)
    if (current_price > target_price) and (current_price > yesteday_ma5):
      markets.append((ticker, korean_name))
  return markets

def get_under_ma5_ma10(tickers):
  markets = []
  for ticker, korean_name in tickers:
    yesteday_ma5 = get_yesterday_ma(ticker, 5)
    yesteday_ma10 = get_yesterday_ma(ticker, 10)
    current_price = get_current_price(ticker)
    if (current_price < yesteday_ma5) and (current_price < yesteday_ma10):
      markets.append((ticker, korean_name))
  return markets

def get_under_every_stats(tickers):
  markets = []
  for ticker, korean_name in tickers:
    yesteday_ma5 = get_yesterday_ma(ticker, 5)
    yesteday_ma10 = get_yesterday_ma(ticker, 10)
    yesteday_ma60 = get_yesterday_ma(ticker, 60)
    yesteday_ma120 = get_yesterday_ma(ticker, 120)
    current_price = get_current_price(ticker)
    if (current_price < yesteday_ma5) and (current_price < yesteday_ma10) and (current_price < yesteday_ma60) and (current_price < yesteday_ma120):
      markets.append((ticker, korean_name))
  return markets

def get_target_test(tickers):
  markets = []
  for ticker, korean_name in tickers:
    yesteday_ma5 = get_yesterday_ma(ticker, 5)
    yesteday_ma10 = get_yesterday_ma(ticker, 10)
    target_price = get_target_price(ticker)
    current_price = get_current_price(ticker)
    if ((current_price < yesteday_ma5) or (current_price < yesteday_ma10)) and (current_price > target_price):
      markets.append((ticker, korean_name))
  return markets

tickers = get_tickers(fiat='KRW')

bull = get_bull_market(tickers)
under_ma5_ma10 = get_under_ma5_ma10(tickers)
under_every = get_under_every_stats(tickers)
target_test = get_target_test(tickers)

send_kakao_message('변동성 돌파 + 상승장', bull)
send_kakao_message('이평선 5-10 이하', under_ma5_ma10)
send_kakao_message('이평선 5-10-20-60-120 이하', under_every)
send_kakao_message('특수', target_test)