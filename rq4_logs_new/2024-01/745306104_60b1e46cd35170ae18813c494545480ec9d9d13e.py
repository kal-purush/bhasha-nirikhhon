#!/usr/bin/env python
# coding: utf-8

# # Expository Analysis:
# Importing all the library's for executing project

# In[6]:


# imported all the libraries 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statistics
import statsmodels.api as smf


# # Reading Power Consumption file to gather all necessary Data information

# In[7]:


# reading the csv file to access the database
power=pd.read_csv('Tetuan_City_power_consumption.csv')
power


# # Attiributes/characteristics of the Data Set

# In[8]:


#shape of the whole data giving the number of columns and rows 
power.shape


# In[9]:


# Basic info on our data telling us how many of each null types their are and the data-types for each variable column
power.info() 


# In[10]:


# overview of dispersion of data giving statistical information:
#mean-central tendency of the average of the values
#standard-deviation-variation within data and how far its values are away from the mean
#min/max values- Range of data-measure of how spread out your data is
power.describe()


# In[11]:


# sum of all the null values from 3 dependent variables(Temperature/humidity/Wind Speed)in regards to Zone 1,2,3 consumption

power.isnull().sum()


# # Renaming the columns and cleaning up the column-names

# In[12]:


# Renaming Data-Frame to clean up the data and makes it more understandable
power.rename(columns={'Zone 1 Power Consumption':'Zone 1 Consumption'},inplace=True)
power.rename(columns={'Zone 2 Power Consumption':'Zone 2 Consumption'},inplace=True)
power.rename(columns={'Zone 3 Power Consumption':'Zone 3 Consumption'},inplace=True)
power.rename(columns={'Date Time': 'DateTime'},inplace=True)


# # Data Visualization comparing our Different variables with each Power consumption zone

# In[13]:


#Data Visualization: 
# Plotting a scatter plot based on how Zone 1,2, and 3 power consumption is being affected by temperature.

x=power['Temperature']
y=power['Zone 1 Consumption']
plt.scatter(x,y,label='temperature vs Zone 1')
plt.xlabel('Temperature')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing temperature')

x=power['Temperature']
y=power['Zone 2 Consumption']
plt.scatter(x,y,label='temperature vs Zone 2')
plt.xlabel('Temperature')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing temperature')


x=power['Temperature']
y=power['Zone 3 Consumption']
plt.scatter(x,y,label='temperature vs Zone 3')
plt.xlabel('Temperature')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing temperature')
plt.legend()


# In[14]:


#Data Visualization: 
# Plotting a scatter plot based on how Zone 1,2, and 3 power consumption is being affected by Wind Speed.
x=power['Wind Speed']
y=power['Zone 1 Consumption']
plt.scatter(x,y,label='Wind Speed vs Zone 1')
plt.xlabel('Wind Speed')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing Wind Speed')

x=power['Wind Speed']
y=power['Zone 2 Consumption']
plt.scatter(x,y,label='Wind Speed vs Zone 2')
plt.xlabel('Wind Speed')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing Wind Speed')


x=power['Wind Speed']
y=power['Zone 3 Consumption']
plt.scatter(x,y,label='Wind Speed vs Zone 3')
plt.xlabel('Wind Speed')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing Wind Speed')
plt.legend()


# In[15]:


#Data Visualization: 
# Plotting a scatter plot based on how Zone 1,2, and 3 power consumption is being affected by Humidity.
x=power['Humidity']
y=power['Zone 1 Consumption']
plt.scatter(x,y,label='Humidity vs Zone 1')
plt.xlabel('Temperature')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing temperature')

x=power['Humidity']
y=power['Zone 2 Consumption']
plt.scatter(x,y,label='Humidity vs Zone 2',alpha=0.5)
plt.xlabel('Temperature')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing temperature')


x=power['Humidity']
y=power['Zone 3 Consumption']
plt.scatter(x,y,label='Humidity vs Zone 3')
plt.xlabel('Temperature')
plt.ylabel('Power Consumption')
plt.title('Power consumption versus increasing Humidity')

plt.legend()


# # Heat Map giving coorelation coeffiecients/relationships between data set.

# In[16]:


# heat map-giving coorelation values based on 3 variables(Temperature,Humidity, and Wind Speed)-(strong/weak coorelation)
corr=power.corr()
sns.heatmap(corr,annot=True,cmap='Reds')
plt.title('coorelation values for different variables affecting the power consumption')


# # Extracting all the columns except Date/Time to visualize different variables and how coorelated they are to each other

# In[17]:


#only columns except DateTime
# Extracting only the columns except for DateTime column
columns_1=power.iloc[:,1:7] 


# In[18]:


# using seaborn library to give visual representation of multiple variables within your data
sns.pairplot(columns_1)


# # Regression Fit Summaries for the different variables against Zone 1,2,3 consumption

# In[19]:


# calculating alpha and beta values and  important statsitics about the data as well as 
#regression coefficients/standard errors within the data between our three variables and zone 1 consumption
x=power[['Temperature','Humidity','Wind Speed']]
y=power['Zone 1 Consumption']
x=smf.add_constant(x)
regression_line=smf.OLS(y,x)
regression_fit=regression_line.fit()
predicted=regression_fit.predict()
regression_fit.summary()


# In[20]:


# calculating alpha and beta values and  important statsitics about the data as well as 
#regression coefficients/standard errors within the data between our three variables and zone 2 consumption
x=power[['Temperature','Humidity','Wind Speed']]
y=power['Zone 2 Consumption']
x=smf.add_constant(x)
regression_line=smf.OLS(y,x)
regression_fit=regression_line.fit()
predicted=regression_fit.predict()
regression_fit.summary()


# In[21]:


# calculating alpha and beta values and  important statsitics about the data as well as 
#regression coefficients/standard errors within the data between our three variables and zone 3 consumption
x=power[['Temperature','Humidity','Wind Speed']]
y=power['Zone 3 Consumption']
x=smf.add_constant(x)
regression_line=smf.OLS(y,x)
regression_fit=regression_line.fit()
predicted=regression_fit.predict()
regression_fit.summary()


# # Model Build

# In[22]:


#Importing libraries

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sklearn


# In[23]:


data=pd.read_csv("Tetuan_City_power_consumption.csv")
data=data.rename(columns={"Wind Speed":"Wind","Zone 1 Power Consumption":"power1","Zone 2 Power Consumption":"power2",'Zone 3 Power Consumption':'power3'})
data.head()


# In[24]:


data['Temperature (std units)']=(data['Temperature']-np.mean(data['Temperature']))/np.std(data['Temperature'])
data['Humidity (std units)']=(data['Humidity']-np.mean(data['Humidity']))/np.std(data['Humidity'])
data['Wind (std units)']=(data['Wind']-np.mean(data['Wind']))/np.std(data['Wind'])
data['power1 (std units)']=(data['power1']-np.mean(data['power1']))/np.std(data['power1'])
data['power2 (std units)']=(data['power2']-np.mean(data['power2']))/np.std(data['power2'])
data['power3 (std units)']=(data['power3']-np.mean(data['power3']))/np.std(data['power3'])
data.head()


# In[25]:


#Randomly shuffling (without replacement) all the rows in the dataframe 'data'. After shuffling, picking the first 66.66% of 
#the rows as the training set and the rest 33.33% of the rows as the test_set. 

random = data.sample(52416, replace = False)
training_set = random.iloc[0:34944, :]
test_set = random.iloc[34944:, :]

print(len(training_set))
print(len(test_set))


# # Model for Zone 1

# In[26]:


#Creating necessary matrices for training and test sets to calculate the cost function

x_train1 = training_set.iloc[:, 7:10].values
y_train1 = training_set.iloc[:, 10].values
y_train1 = np.reshape(y_train1, (len(y_train1), 1))

x_test1 = test_set.iloc[:, 7:10].values
y_test1 = test_set.iloc[:, 10].values
y_test1 = np.reshape(y_test1, (len(y_test1), 1))

print("x_train_Shape:", np.shape(x_train1))
print("y_train_Shape:", np.shape(y_train1))

print("x_test_Shape:", np.shape(x_test1))
print("y_test_Shape:", np.shape(y_test1))


# In[27]:


#Transposing and stacking a row of ones vertically to the 'x_train and x_test' for the purposes of vectorization

x_train_trans1 = np.transpose(x_train1)
x_train_Aug1 = np.vstack((np.ones((1,len(x_train1))),x_train_trans1))
print("x_train_Aug:", np.shape(x_train_Aug1))

x_test_trans1 = np.transpose(x_test1)
x_test_Aug1 = np.vstack((np.ones((1,len(x_test1))),x_test_trans1))
print("x_test_Aug:", np.shape(x_test_Aug1))

x_train_Aug1


# In[28]:


#Defining theta as an array of zeros

theta1 = np.zeros((4,1))
print("theta:", np.shape(theta1))


# In[29]:


#Implementing the gradient descent algorithm

no_of_iter1 = np.arange(1, 15000)
alpha1 = 0.003
m_train1 = len(x_train1)
m_test1 = len(x_test1)

costfunc1 = []

#Iteration loop
for i in no_of_iter1:
    z1 = np.transpose(theta1)@x_train_Aug1 #Hypothesis function
    cf1 = (1/(2*m_train1))*(np.sum((np.transpose(z1)-y_train1)**2)) #Cost function
    costfunc1.append(cf1) #Appending cost function
    delthetaj1 = (1/m_train1)*((x_train_Aug1)@(np.transpose(z1)-y_train1)) #Derivative of cost function
    theta1 = theta1 - (alpha1*delthetaj1) #Updating theta values
    
print(len(costfunc1))
print(theta1)


# In[30]:


#Testing the model on the training set

h_theta1 = np.transpose(theta1)@x_train_Aug1
y_train_pred1 = np.transpose(h_theta1)

plt.scatter(y_train1, y_train_pred1, color='r')
plt.plot([y_train1.min(), y_train1.max()], [y_train_pred1.min(), y_train_pred1.max()], color = 'black', lw=2)
plt.xlabel("Y_train")
plt.ylabel("Y_train_pred")
plt.title("Predictions vs. actual values in the training set")


# In[31]:


#Testing the model on the test set

h_theta1 = np.transpose(theta1)@x_test_Aug1
y_test_pred1 = np.transpose(h_theta1)

plt.scatter(y_test1, y_test_pred1, color='r')
plt.plot([y_test1.min(), y_test1.max()], [y_test_pred1.min(), y_test_pred1.max()], color = 'black', lw=2)
plt.xlabel("Y_test")
plt.ylabel("Y_test_pred")
plt.title("Predictions vs. actual values in the test set")


# In[32]:


#Plotting cost function vs. number of iterations

plt.plot(no_of_iter1[:14999],costfunc1[:14999],color='r',linewidth = '3')
plt.xlabel("Number of iterations")
plt.ylabel("Cost function")
plt.title("Cost function vs. number of iterations")


# In[33]:


#Computing the MSE and the RMSE values for the predictions made on the training set

MSE_train1 = (1/m_train1)*(np.sum((y_train1-y_train_pred1)**2))
RMSE_train1 = np.sqrt(MSE_train1)

print(MSE_train1)
print(RMSE_train1)


# In[34]:


#Computing the MSE and the RMSE values for the predictions made on the test set

MSE_test1 = (1/m_test1)*(np.sum((y_test1-y_test_pred1)**2))
RMSE_test1 = np.sqrt(MSE_test1)

print(MSE_test1)
print(RMSE_test1)


# # Model for Zone 2

# In[35]:


#Creating necessary matrices for training and test sets to calculate the cost function

x_train2 = training_set.iloc[:, 7:10].values
y_train2 = training_set.iloc[:, 11].values
y_train2 = np.reshape(y_train2, (len(y_train2), 1))

x_test2 = test_set.iloc[:, 7:10].values
y_test2 = test_set.iloc[:, 11].values
y_test2 = np.reshape(y_test2, (len(y_test2), 1))

print("x_train_Shape:", np.shape(x_train2))
print("y_train_Shape:", np.shape(y_train2))

print("x_test_Shape:", np.shape(x_test2))
print("y_test_Shape:", np.shape(y_test2))


# In[36]:


#Transposing and stacking a row of ones vertically to the 'x_train and x_test' for the purposes of vectorization

x_train_trans2 = np.transpose(x_train2)
x_train_Aug2 = np.vstack((np.ones((1,len(x_train2))),x_train_trans2))
print("x_train_Aug:", np.shape(x_train_Aug2))

x_test_trans2 = np.transpose(x_test2)
x_test_Aug2 = np.vstack((np.ones((1,len(x_test2))),x_test_trans2))
print("x_test_Aug:", np.shape(x_test_Aug2))

x_train_Aug2


# In[37]:


#Defining theta as an array of zeros

theta2 = np.zeros((4,1))
print("theta:", np.shape(theta2))


# In[38]:


#Implementing the gradient descent algorithm

no_of_iter2 = np.arange(1, 15000)
alpha2 = 0.003
m_train2 = len(x_train2)
m_test2 = len(x_test2)

costfunc2 = []

#Iteration loop
for i in no_of_iter2:
    z2 = np.transpose(theta2)@x_train_Aug2 #Hypothesis function
    cf2 = (1/(2*m_train2))*(np.sum((np.transpose(z2)-y_train2)**2)) #Cost function
    costfunc2.append(cf2) #Appending cost function
    delthetaj2 = (1/m_train2)*((x_train_Aug2)@(np.transpose(z2)-y_train2)) #Derivative of cost function
    theta2 = theta2 - (alpha2*delthetaj2) #Updating theta values
    
print(len(costfunc2))
print(theta2)


# In[39]:


#Testing the model on the training set

h_theta2 = np.transpose(theta2)@x_train_Aug2
y_train_pred2 = np.transpose(h_theta2)

plt.scatter(y_train2, y_train_pred2, color='r')
plt.plot([y_train2.min(), y_train2.max()], [y_train_pred2.min(), y_train_pred2.max()], color = 'black', lw=2)
plt.xlabel("Y_train")
plt.ylabel("Y_train_pred")
plt.title("Predictions vs. actual values in the training set")


# In[40]:


#Testing the model on the test set

h_theta2 = np.transpose(theta2)@x_test_Aug2
y_test_pred2 = np.transpose(h_theta2)

plt.scatter(y_test2, y_test_pred2, color='r')
plt.plot([y_test2.min(), y_test2.max()], [y_test_pred2.min(), y_test_pred2.max()], color = 'black', lw=2)
plt.xlabel("Y_test")
plt.ylabel("Y_test_pred")
plt.title("Predictions vs. actual values in the test set")


# In[41]:


#Plotting cost function vs. number of iterations

plt.plot(no_of_iter2[:14999],costfunc2[:14999],color='r',linewidth = '3')
plt.xlabel("Number of iterations")
plt.ylabel("Cost function")
plt.title("Cost function vs. number of iterations")


# In[42]:


#Computing the MSE and the RMSE values for the predictions made on the training set

MSE_train2 = (1/m_train2)*(np.sum((y_train2-y_train_pred2)**2))
RMSE_train2 = np.sqrt(MSE_train2)

print(MSE_train2)
print(RMSE_train2)


# In[43]:


#Computing the MSE and the RMSE values for the predictions made on the test set

MSE_test2 = (1/m_test2)*(np.sum((y_test2-y_test_pred2)**2))
RMSE_test2 = np.sqrt(MSE_test2)

print(MSE_test2)
print(RMSE_test2)


# # Model for Zone 3

# In[44]:


#Creating necessary matrices for training and test sets to calculate the cost function

x_train = training_set.iloc[:, 7:10].values
y_train = training_set.iloc[:, 12].values
y_train = np.reshape(y_train, (len(y_train), 1))

x_test = test_set.iloc[:, 7:10].values
y_test = test_set.iloc[:, 12].values
y_test = np.reshape(y_test, (len(y_test), 1))

print("x_train_Shape:", np.shape(x_train))
print("y_train_Shape:", np.shape(y_train))

print("x_test_Shape:", np.shape(x_test))
print("y_test_Shape:", np.shape(y_test))


# In[45]:


#Transposing and stacking a row of ones vertically to the 'x_train and x_test' for the purposes of vectorization

x_train_trans = np.transpose(x_train)
x_train_Aug = np.vstack((np.ones((1,len(x_train))),x_train_trans))
print("x_train_Aug:", np.shape(x_train_Aug))

x_test_trans = np.transpose(x_test)
x_test_Aug = np.vstack((np.ones((1,len(x_test))),x_test_trans))
print("x_test_Aug:", np.shape(x_test_Aug))

x_train_Aug


# In[46]:


#Defining theta as an array of zeros

theta = np.zeros((4,1))
print("theta:", np.shape(theta))


# In[47]:


#Implementing the gradient descent algorithm

no_of_iter = np.arange(1, 15000)
alpha = 0.003
m_train = len(x_train)
m_test = len(x_test)

costfunc = []

#Iteration loop
for i in no_of_iter:
    z = np.transpose(theta)@x_train_Aug #Hypothesis function
    cf = (1/(2*m_train))*(np.sum((np.transpose(z)-y_train)**2)) #Cost function
    costfunc.append(cf) #Appending cost function
    delthetaj = (1/m_train)*((x_train_Aug)@(np.transpose(z)-y_train)) #Derivative of cost function
    theta = theta - (alpha*delthetaj) #Updating theta values
    
print(len(costfunc))
print(theta)


# In[48]:


#Testing the model on the training set

h_theta = np.transpose(theta)@x_train_Aug
y_train_pred = np.transpose(h_theta)

plt.scatter(y_train, y_train_pred, color='r')
plt.plot([y_train.min(), y_train.max()], [y_train_pred.min(), y_train_pred.max()], color = 'black', lw=2)
plt.xlabel("Y_train")
plt.ylabel("Y_train_pred")
plt.title("Predictions vs. actual values in the training set")


# In[49]:


#Testing the model on the test set

h_theta = np.transpose(theta)@x_test_Aug
y_test_pred = np.transpose(h_theta)

plt.scatter(y_test, y_test_pred, color='r')
plt.plot([y_test.min(), y_test.max()], [y_test_pred.min(), y_test_pred.max()], color = 'black', lw=2)
plt.xlabel("Y_test")
plt.ylabel("Y_test_pred")
plt.title("Predictions vs. actual values in the test set")


# In[50]:


#Plotting cost function vs. number of iterations

plt.plot(no_of_iter[:14999],costfunc[:14999],color='r',linewidth = '3')
plt.xlabel("Number of iterations")
plt.ylabel("Cost function")
plt.title("Cost function vs. number of iterations")


# In[51]:


#Computing the MSE and the RMSE values for the predictions made on the training set

MSE_train = (1/m_train)*(np.sum((y_train-y_train_pred)**2))
RMSE_train = np.sqrt(MSE_train)

print(MSE_train)
print(RMSE_train)


# In[52]:


#Computing the MSE and the RMSE values for the predictions made on the test set

MSE_test = (1/m_test)*(np.sum((y_test-y_test_pred)**2))
RMSE_test = np.sqrt(MSE_test)

print(MSE_test)
print(RMSE_test)


# # Assessing data model Quality:
# 
# 1. Using Residual plots for test sets of all Zones
# 2. Comparing RMSE values for test sets of all Zones

# ### Residual Plot for Zone 1:

# In[53]:


Residual1=y_test1 - y_test_pred1
plt.scatter(y_test_pred1,Residual1,color='r')
plt.plot([-1.5, 2], [0,0], color = 'blue', lw = 3)
plt.xlabel('Predicted Test Value for Zone 1')
plt.ylabel('Residuals for Zone 1')
plt.title('Predicted test values vs Residuals to check model quality')


# ### Residual Plot for Zone 2:

# In[54]:


Residual2=y_test2 - y_test_pred2
plt.scatter(y_test_pred2,Residual2,color='r')
data['power1 (std units)']=(data['power1']-np.mean(data['power1']))/np.std(data['power1'])
plt.plot([-1.5, 2], [0,0], color = 'blue', lw = 3)
plt.xlabel('Predicted Test Value for Zone 2')
plt.ylabel('Residuals for Zone 2')
plt.title('Predicted test values vs Residuals to check model quality')


# ### Residual Plot for Zone 3:

# In[55]:


Residual=y_test - y_test_pred
plt.scatter(y_test_pred,Residual,color='r')
plt.plot([-1.5, 2], [0,0], color = 'blue', lw = 3)
plt.xlabel('Predicted Test Value for Zone 3')
plt.ylabel('Residuals for Zone 3')
plt.title('Predicted test values vs Residuals to check model quality')


# In[56]:


print('RMSE value of model for Zone 1:',RMSE_test1)
print('RMSE value of model for Zone 2:',RMSE_test2)
print('RMSE value of model for Zone 3:',RMSE_test)


# ### By looking at the graphs of residuals and the randomness of their distributions, and RMSE values, we can see that the model is best fitted for Zone 3, followed by Zone 1, and then Zone 2.

# ## Building the input data interface for using the "best" model(s):
# ### Since the model works best for Zone 3, we will use its model to allow users to enter input parameters and return an estimated power consumption. The function we will make uses coefficients for temperature, humidity, and wind, and the intercept to provide a prediction for the user.

# In[57]:


def predict(temp,humidity,wind): 
    p=(516.2860*temp)-(6.8909*humidity)+(169.5082*wind)+8262.2755
    return p

temp=float(input('Enter the observed temperature for Zone 3: '))
humidity=float(input('Enter the observed humidity for Zone 3: '))
wind=float(input('Enter the observed wind for Zone 3: '))

prediction= predict(temp,humidity,wind)
print("The prediction for ZOne 3 is: ", prediction)


# ### Making prediction functions for zones 1 and 2 as well:

# In[58]:


#ZONE 1 PREDICT FUNCTION

def predict1(temp1,humidity1,wind1): 
    p1=(507.4127*temp1)-(47.1971*humidity1)-(133.5183*wind1)+2.628e+04
    return p1

temp1=float(input('Enter the observed temperature for Zone 1: '))
humidity1=float(input('Enter the observed humidity for Zone 1: '))
wind1=float(input('Enter the observed wind for Zone 1: '))

prediction1= predict1(temp1,humidity1,wind1)
print("The prediction for Zone 1 is: ", prediction1)


# In[59]:


#ZONE 2 PREDICT FUNCTION

def predict2(temp2,humidity2,wind2): 
    p2=(294.3498*temp2)-(49.3893*humidity2)-(67.8995*wind2)+1.901e+04
    return p2

temp2=float(input('Enter the observed temperature for Zone 2: '))
humidity2=float(input('Enter the observed humidity for Zone 2: '))
wind2=float(input('Enter the observed wind for Zone 2: '))

prediction2= predict2(temp2,humidity2,wind2)
print("The prediction for Zone 2 is: ", prediction2)


# In[60]:


zone1_pred1=predict1(temp1,humidity1,wind1)
zone1_pred2=predict1(temp2,humidity2,wind2)
zone1_pred3=predict1(temp,humidity,wind)

zone2_pred1=predict2(temp1,humidity1,wind1)
zone2_pred2=predict2(temp2,humidity2,wind2)
zone2_pred3=predict2(temp,humidity,wind)

zone3_pred1=predict(temp1,humidity1,wind1)
zone3_pred2=predict(temp2,humidity2,wind2)
zone3_pred3=predict(temp,humidity,wind)

new_row = pd.DataFrame({'Temperature':[temp1,temp2,temp],'Humidity':[humidity1,humidity2,humidity],'Wind Speed':[wind1,wind2,wind],
              'Zone 1 Consumption': [zone1_pred1,zone1_pred2,zone1_pred3],
              'Zone 2 Consumption': [zone2_pred1,zone2_pred2,zone2_pred3],
              'Zone 3 Consumption' : [zone3_pred1,zone3_pred2,zone3_pred3]})

power = power.append(new_row, ignore_index=True)
power


# In[61]:


x=RMSE_test1*np.std(power['Zone 1 Consumption'])+np.mean(power['Zone 1 Consumption'])
print('RMSE for zone 1',x)
Confidence_upper=x+1.96*RMSE_test1
Confidence_lower=x-1.96*RMSE_test1
print('Confidence upper/lower for zone 1',Confidence_upper)
print('Confidence lower for zone 1',Confidence_lower)


# In[62]:


y=RMSE_test2*np.std(power['Zone 2 Consumption'])+np.mean(power['Zone 2 Consumption'])
print('RMSE for zone 2',y)
Confidence_upper=x+1.96*RMSE_test2
Confidence_lower=x-1.96*RMSE_test2
print('Confidence upper/lower for zone 2',Confidence_upper)
print('Confidence lower for zone 2',Confidence_lower)


# In[63]:


z=RMSE_test*np.std(power['Zone 3 Consumption'])+np.mean(power['Zone 3 Consumption'])
print('RMSE for zone 3',z)
Confidence_upper=z+1.96*RMSE_test
Confidence_lower=z-1.96*RMSE_test
print('Confidence upper/lower for zone 3',Confidence_upper)
print('Confidence lower for zone 3',Confidence_lower)


# In[ ]:





# In[ ]:





# In[ ]:



