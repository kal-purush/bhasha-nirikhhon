import 






def get_orders(positions, symbol):
	Orders = pd.DataFrame(columns=["Symbol","Order","Shares"])
	holding = 0

	for index, position in positions.iteritems():
		if position == "long":
			if holding==0:
				Orders.loc[index] = [symbol, "BUY", int(1000)]
			if holding==-1000:
				Orders.loc[index] = [symbol, "BUY", int(2000)]
			holding=1000
		if position == "short":
			if holding==0:
				Orders.loc[index] = [symbol, "SELL", int(1000)]
			if holding==1000:
				Orders.loc[index] = [symbol, "SELL", int(2000)]
			holding=-1000
		if position == "close":
			if holding > 0:
				Orders.loc[index] = [symbol, "SELL", holding]
			if holding < 0:
				Orders.loc[index] = [symbol, "BUY", holding]
			holding=0

	return Orders



def benchmark(sd=dt.datetime(2010,1,1), ed=dt.datetime(2011,12,31)):
	price = get_data(["JPM"], pd.date_range(sd, ed))
	# buy 1000 shares in sd
	df = pd.DataFrame(columns=["Date", "Symbol","Order","Shares"])
	df.loc[sd] = [sd,"JPM",'BUY',1000]
	df.loc[ed] = [ed,"JPM",'BUY',0]
	return df


def optimal_strategy(symbol = "AAPL", sd=dt.datetime(2010,1,1), ed=dt.datetime(2011,12,31), sv = 100000):
	price = get_data([symbol], pd.date_range(sd, ed))
	del price["SPY"]
	price["tomorrow"] = price[symbol].shift(-1)
	last_row = price.loc[ed]
	price = price.dropna()
	price["position_b"] = price[symbol] >= price["tomorrow"]
	price['position'] = np.where(price['position_b']==True, 'short', 'long')
	del price["position_b"]
	last_row["position"] = "close"
	price.loc[ed] = last_row
	Orders = get_orders(price['position'], symbol)	
	Orders["Date"]	= Orders.index 
	return Orders