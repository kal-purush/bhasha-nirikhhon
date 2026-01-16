import pandas as pd
from data_util import offer_rainfall_data

pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)

url = r"D:/weatherAUS.csv"

X,y = offer_rainfall_data(url,2,0,1)