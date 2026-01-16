#Python Script for Biblical Baby Names Project
#Danielle Hill
#danie11e.com
#2015

#Import pandas and numpy and os
import pandas as pd
import numpy as np
import os


#1. Create a list of state abbreviations using file names
files = os.listdir('data/state_names/.')
states = []

for file in files:
	#grap first two letters and append to states array
	states.append(file[:2])


#2. Create DataFrame by concatenating individual state files
pieces = []
columns = ['state', 'gender', 'year', 'name', 'births']

for state in states:
	path = 'data/state_names/%s.txt' % state
	frame = pd.read_csv(path, names=columns) 

	pieces.append(frame)

df = pd.concat(pieces, ignore_index=True)

#3. Add T|F indicator to biblical list
biblical = pd.read_excel('data/biblicalnames.xlsx', 'biblicalnames', index_col=None, na_values=['NA'])
biblical['biblical'] = 'true'


#4. Merge biblical name list to names to show T|F for all names using left join
names = pd.merge(df, biblical, how='left')
#fill in na with false
names['biblical'].fillna('false', inplace=True)


#5. Create a pivot table summarizing the names by year and state
table = pd.pivot_table(names, values='births', index=['year', 'state', 'gender'],columns=['biblical'], aggfunc=np.sum)


#6. Rename table columns to add calculations, add total and add % Biblical
table.columns = ['false', 'true']
table['total'] = table['false'] + table['true']
table["%"] = (table["true"] / (table["true"]+table["false"])) * 100

#Store table and names as csv	for visualization with d3.js and/or Tableau
table.to_csv('bbnames_summary.csv')
names.to_csv('bbnames_raw.csv')