import glob
import pandas as pd
import shutil

csv_files = glob.glob('/shared/scratch/am13743/trees/*.csv')
 
for file in csv_files:
    df = pd.read_csv(file,header=None)
    df = df.dropna(how='all')
    try:
        df_csv_append = df_csv_append.append(df)
    except:
        df_csv_append = df
    print(df_csv_append.shape)
print(df_csv_append)
df_csv_append.to_csv(f'trees_full.csv',header=False, index=False)
shutil.copyfile(f'trees_full.csv', f'../app/src/main/assets/trees.txt')