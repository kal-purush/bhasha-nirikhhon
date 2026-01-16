
from __future__ import absolute_import
import numpy as np



def get_orders(actions):
	"""
	Takes the output of the strategy (actions series which is pd Series)
	outputs a data frame  with 2 columns (order and shares)
	It works on just 1 symbol
	"""
	Orders = pd.DataFrame(columns=["Order","Shares"])
	holding = 0

	for index, position in positions.iteritems():
		if position == "long":
			if holding==0:
				Orders.loc[index] = ["BUY", int(1000)]
			if holding==-1000:
				Orders.loc[index] = ["BUY", int(2000)]
			holding=1000
		if position == "short":
			if holding==0:
				Orders.loc[index] = ["SELL", int(1000)]
			if holding==1000:
				Orders.loc[index] = ["SELL", int(2000)]
			holding=-1000
		if position == "close":
			if holding > 0:
				Orders.loc[index] = ["SELL", holding]
			if holding < 0:
				Orders.loc[index] = ["BUY", holding]
			holding=0
	return Orders



def compute_portvals(Orders, data, start_val = 1000000, commission=9.95, impact=0.005):                                                                
    """
	Takes as input the 
	Orders: data frame with orders
	data: Prices data frame
	start_val: the start value
	commission
	impact
    """ 


                                                    
    orders = Orders.copy()
    # reading orders
    #orders = pd.read_csv(orders_file)
    Dates = orders.index
    start_date = Dates.min()
    end_date = Dates.max()
    symbol = orders.columns[0]
    prices = get_data([symbol], pd.date_range(start_date, end_date))
    del prices["SPY"]


    orders["impact"] = [-impact] * orders.shape[0]
    orders.loc[orders[symbol]==0]=0


    orders["cash"] = orders["impact"]  - orders[symbol] * prices[symbol]  
    orders["cash"][0] = orders["cash"][0] + start_val


    orders = orders.cumsum()
    orders[symbol] =  orders[symbol] * prices[symbol] 
    portvals = orders.sum(axis=1)
    return portvals


def computing_daily_returns(port_val):
    daily_rets = port_val.copy()
    daily_rets[1:] = (port_val[1:] / port_val[:-1]) -1
    daily_rets = daily_rets[1:] 
    return daily_rets


def compute_statistics(port_val):
    cr=(port_val[-1]/port_val[0]) - 1
    daily_rets = computing_daily_returns(port_val)
    adr=daily_rets.mean()
    sddr=daily_rets.std()  
    sr = np.sqrt(252) * adr / sddr
    return cr , adr, sddr, sr



def fill_for_noncomputable_vals(input_data, result_data):
    non_computable_values = np.repeat(
        np.nan, len(input_data) - len(result_data)
        )
    filled_result_data = np.append(non_computable_values, result_data)
    return filled_result_data

