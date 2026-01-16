import pandas

malDataSet = pandas.read_csv('Android_Feats.csv',low_memory=False)

origin_headers = list(malDataSet.columns.values)

total_data = malDataSet[origin_headers[:-1]]
total_data = total_data.as_matrix()
target_strings = malDataSet[origin_headers[-1]]