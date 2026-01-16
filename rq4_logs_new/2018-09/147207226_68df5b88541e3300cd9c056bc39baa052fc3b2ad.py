import pandas as pd
from sklearn.linear_model import LinearRegression
train_file='train.csv'
test_file='predict.csv'
train_data=pd.read_csv(train_file)
test_data=pd.read_csv(test_file)
train_data.drop_duplicates(inplace=True)
test_data.drop_duplicates(inplace=True)
print(train_data.shape, test_data.shape)
y = train_data.pop('prediction_pay_price')
drop = ['user_id']
test_idx=test_data[drop]
train_data=train_data.drop(drop, axis=1)
test_data=test_data.drop(drop, axis=1)
lr = LinearRegression()
lr.fit(train_data, y)
y_prob = lr.predict(test_data)
test_idx['prediction_pay_price'] = y_prob
test_idx[test_idx<0]=0
#test_idx.loc[test_idx['prediction_pay_price']>40454]=70000
print(test_idx.describe())
print(test_idx.shape)
test_idx.to_csv("result.csv", index=None)