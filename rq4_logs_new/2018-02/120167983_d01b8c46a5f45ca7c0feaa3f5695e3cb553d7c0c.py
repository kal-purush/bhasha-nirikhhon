import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
import seaborn as sns
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
min_max_scaler = MinMaxScaler()

#Load data
inputPath = r'D:\code\Bitcoin\bitcoin-historical-data\market-price.csv'
#inputPath = r'D:\code\Bitcoin\bitcoin-historical-data\bitflyerJPY_1-min_data_2017-07-04_to_2018-01-08.csv'
#data = np.array(np.loadtxt(inputPath, delimiter=",", skiprows=(1), usecols=(2,3,4)))
df=pd.read_csv(inputPath,usecols=[1],header=None)

#Split into training and testing
prediction_days = 100
df_train= df[:len(df)-prediction_days]
df_test= df[len(df)-prediction_days:]


#Normalize and prepare for training
training_set = df_train.values
training_set = min_max_scaler.fit_transform(training_set)

x_train = training_set[0:len(training_set)-1] #The only one feature of X is t
y_train = training_set[1:len(training_set)]   #Y is t+1
x_train = np.reshape(x_train, (len(x_train), 1, 1))

#Train model
num_units = 4
activation_function = 'sigmoid'
optimizer = 'adam'
loss_function = 'mean_squared_error'
batch_size = 5
num_epochs = 200
drop = 0.2

# Initialize the RNN
regressor = Sequential()

# Adding the input layer and the LSTM layer
regressor.add(LSTM(units = num_units, activation = activation_function,dropout = drop, input_shape=(None, 1)))

# Adding the output layer
regressor.add(Dense(units = 1))

# Compiling the RNN
regressor.compile(optimizer = optimizer, loss = loss_function)

# Using the training set to train the model
regressor.fit(x_train, y_train, batch_size = batch_size, epochs = num_epochs)

#4. Predict price
test_set = df_test.values

inputs = np.reshape(test_set, (len(test_set), 1))
inputs = min_max_scaler.transform(inputs)
inputs = np.reshape(inputs, (len(inputs), 1, 1))

predicted_price = regressor.predict(inputs)
predicted_price = min_max_scaler.inverse_transform(predicted_price)

#visualization
plt.figure(dpi=80, facecolor = 'w', edgecolor = 'k')

plt.plot(test_set[:, 0], color='red', label='Real BTC Price')
plt.plot(predicted_price[:, 0], color = 'blue', label = 'Predicted BTC Price')
plt.title('BTC Price Prediction')
plt.xlabel('Time')
plt.ylabel('BTC Price(USD)')
plt.legend(loc = 'best')
plt.show()



