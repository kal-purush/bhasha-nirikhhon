import pandas as pd

path = 'data/data.csv'

d_f = pd.read_csv(path)
a = d_f.head()

print(d_f['Economic_Impact'])
