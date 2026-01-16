import pandas as pd
import numpy as np
import random
import sys

random.seed(42)

if len(sys.argv) < 3:
    print("Not Enough Arguments")
    sys.exit()

input_file = sys.argv[1]
output_file = sys.argv[2]

print("Loading : ",input_file)
df = pd.read_csv(input_file, header=None,delimiter=' ')

data    = df[[1,2]]
mapping = {}
data_p  = []

for row in data.itertuples():
    temp = mapping.get(row[1],False)
    if temp is False:
        mapping[row[1]] = len(mapping)
    
    temp = mapping.get(row[2],False)
    if temp is False:
        mapping[row[2]] = len(mapping)
    
for row in data.itertuples():
    i = mapping.get(row[1],False)
    j = mapping.get(row[2],False)
    if i is False or j is False:
        print("Error -- no mapping found.")
    data_p.append([i,j])

random.shuffle(data_p)

#reallocate memory
df = pd.DataFrame(data_p)

print("Outputing csv : ",output_file)
df.to_csv(output_file,sep=',',index=False,index_label=False,header=False)