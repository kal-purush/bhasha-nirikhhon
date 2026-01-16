export type UpbitCandle = {
  market: string;
  candle_date_time_utc: string;
  candle_date_time_kst: string;
  opening_price: number;
  high_price: number;
  low_price: number;
  trade_price: number;
  timestamp: number;
  candle_acc_trade_price: number;
  candle_acc_trade_volume: number;
  prev_closing_price?: number;
  change_price?: number;
  change_rate?: number;
  converted_trade_price?: number;
  unit?: number;
  first_day_of_period?: number;
};

export type UpbitCandlesParams = {
  market: string;
  candleType: string;
  count: number;
  minute: number;
};

/*
1s
1m
3m
5m
15m
30m
1h
2h
4h
6h
8h
12h
1d
3d
1w
1M
*/

export type BinanceCandlesParams = {
  symbol: string;
  interval: string;
};