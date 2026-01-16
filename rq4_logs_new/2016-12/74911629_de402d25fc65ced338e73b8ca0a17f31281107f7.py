import numpy as np
import pandas as pd

from pandas import DataFrame, Series 

# from pandas_datareader.data import DataReader
#from datetime import date
#from dateutil.relativedelta import relativedelta

# Set some pandas options
#pd.set_option('display.notebook_repr_html', False)
#pd.set_option('display.max_columns', 10)
#pd.set_option('display.max_rows', 10)

optionFile = pd.read_csv("bb_options_20020208.csv")
#print(optionFile.head(30))
#print(optionFile.tail())

applRow = optionFile[optionFile.UnderlyingSymbol == "AAPL"]
print(applRow)