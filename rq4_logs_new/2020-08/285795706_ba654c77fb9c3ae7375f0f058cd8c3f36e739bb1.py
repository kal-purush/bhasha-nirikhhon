import pandas as pd
import xlrd
dfs = pd.read_excel('Data.xlsx')

print(dfs['OCR'])
print(dfs['Path'])
print(dfs.head())
print(dfs['OCR'][1346], dfs['Path'][1346])