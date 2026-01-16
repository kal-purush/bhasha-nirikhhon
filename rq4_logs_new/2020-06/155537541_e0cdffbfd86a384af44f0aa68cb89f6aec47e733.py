commands = open("timeSeries.py").read()
exec(commands)
from pandas.core.common import flatten

out_min = [scaler.data_min_[np.r_[feat]] for feat in time_series_feature_columns]
out_min.append([scaler.data_min_[np.r_[feat]] for feat in target_columns])


out_max = [scaler.data_max_[np.r_[feat]] for feat in time_series_feature_columns]
out_max.append([scaler.data_max_[np.r_[feat]] for feat in target_columns])


file_output = open('min_max.txt', 'w')
print('C minimums:', file=file_output)
for x in list(flatten(out_min)):
  print (x, file=file_output) 
print('C maximums:', file=file_output)
for x in list(flatten(out_max)):
  print (x, file=file_output) 

file_output.close()