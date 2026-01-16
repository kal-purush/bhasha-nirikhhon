import pandas as pd
from time import mktime, strptime
from datetime import datetime

sfc = pd.read_csv("/Users/jasondamiani/Desktop/kaggle/SF Crime/train.csv")

# for date in sfc["Dates"]:
#   try:
#     result = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
#   except:
#     print date

print sfc[sfc.X > -122]