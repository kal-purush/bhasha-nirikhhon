import pandas as pd
import numpy as np

cd1=pd.read_csv('Combined_data_1.csv')
cd2=pd.read_csv('Combined_data_2.csv')

merged_cd = pd.merge(cd2,cd1,on=('Series Name','Series Code','Country Name','Country Code'))
print(merged_cd.dtypes)



merged_cd.to_csv('Merged_Combined_data.csv')