import pandas as pd
import numpy as np

df = pd.read_csv('loansData_clean.csv',header=1; low_memory=False)

#Converts String to a datetime object
df['issue_d_format'] = pd.to_datetime(df['issue_d])