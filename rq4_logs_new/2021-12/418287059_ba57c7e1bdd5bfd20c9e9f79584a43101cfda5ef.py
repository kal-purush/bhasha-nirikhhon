import os
import glob
import pandas as pd
os.chdir("D:\Documents\Documents\DATA606")

extension = 'csv'
all_filenames = [i for i in glob.glob('bitcoin_*.{}'.format(extension))]

#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv
combined_csv.to_csv( "combined_bitcoin.csv", index=False, encoding='utf-8-sig')