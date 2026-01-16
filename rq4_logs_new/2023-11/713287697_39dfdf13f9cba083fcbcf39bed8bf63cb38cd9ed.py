import pandas as pd
from shutil import get_terminal_size
from sys import argv, executable

# check arguments
argc = len(argv)
if argc != 2:
  raise TypeError(f'\
Program expected exact 2 arguments, got {argc}. \
Usage: {executable} {argv[0]} <data_path>')

# get arguments
data_path = argv[1]

# read data
data = pd.read_csv(data_path)

# print data column names
term_width = get_terminal_size().columns
print('Column Names')
cell_width_max = data.columns.map(lambda col: len(col)).max() + 1
cells_per_row = term_width // cell_width_max
for index, col in enumerate(data.columns):
  print(
    f'{col:{cell_width_max}}',
    end='\n' if index % cells_per_row == cells_per_row - 1 else '',
  )
print('\n')

# print every column's value counts
for col in data.columns:
  print(data[col].value_counts(normalize=True, dropna=False), end='\n\n')