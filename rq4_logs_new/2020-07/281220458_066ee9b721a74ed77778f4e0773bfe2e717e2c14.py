from mc_cellsearch import *
import sys, pickle

with open(sys.argv[1], 'rb') as f: res = pickle.load(f)

for c in res.cells:
  print(c.uc)
  print(c.average_score())
  print(c.cumul_score)

