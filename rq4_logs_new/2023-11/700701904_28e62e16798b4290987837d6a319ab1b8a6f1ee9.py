#!/usr/bin/env python
# coding: utf-8

# ### Date: 2023-10-05
# ### Author: Jennifer Fortuny I Zhan
# ### Content: Project 2 Data Preparation

# The variable we would like to predict, i.e. our target (the dependent variable) is income
# - Due to the nature of our data it will end up being a very standard classification problem: is income =<50K or >50K?
# - Regardless, the regression step should be interesting too.
# - In the regression step, it would be possible to assign a probability score of predicting a numeric score to represent the likelihood of being in the >50K or <=50K income level. Higher scores mean greater likelyhood of being in the >50K category.
# 
# The variables we will use to make that prediction, i.e. our features are: age, education-num, hours-per-week, workclass, and occupation (these are the independent variables).
# 
# I will begin by splitting the entire dataset into a training set and a testing set. I will train my linear regression model on one subset of the data, then test its performance on another, unseen subset.

# Import required libraries

# In[9]:


from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
import pandas as pd


# Load the data from the CSV file, then save it as a DataFrame.

# In[10]:


census_data = pd.read_csv('../filtered_data.csv')
census_df = pd.DataFrame(census_data)


# #### Scaling Numerical Independent Variables
# 
# Use StandardScaler to scale the numerical independent variables to that each has mean of 0 and s.d. of 1.
# These varibles are: age, education-num, and hours-per-week.

# In[11]:


# Initialise StandardScaler
scaler = StandardScaler()

# Fit and transform the numeric independent variables
scaled_numeric_ind = scaler.fit_transform(
    census_df[['age', 'education-num', 'hours-per-week']])

# Save the converted data back into a dataframe
scaled_numeric_ind_df = pd.DataFrame(
    scaled_numeric_ind,
    columns = ['age', 'education-num', 'hours-per-week'])

print("Original Numeric Independent Data, first row:")
print(census_df.head(1))
print("Scaled Numeric Independent Data, first row:")
print(scaled_numeric_ind_df.head(1))


# #### Encoding Categorical Independent Variables
# 
# Use one-hot encoding () to convert the Categorical Independent Variables: workclass and occupation.

# In[12]:


# Initialise OneHotEncoder
encoder = OneHotEncoder(sparse_output=False)

# Fit and transform the categorical independent variables
encoded_categorical_ind = encoder.fit_transform(
    census_df[["workclass", "occupation"]])

# Convert the result back into a DataFrame
encoded_categorical_ind_df = pd.DataFrame(
    encoded_categorical_ind,
    columns=encoder.get_feature_names_out(["workclass", "occupation"]))

print("Encoded Categorical Independent Data:, first two rows.")
print(encoded_categorical_ind_df.head(2))


# #### Create Training Data
# 

# Combine the scaled numerical and one-hot encoded categorical independent vairables back into one dataframe. This will be the data frame we use for model training.

# In[13]:


# Add two dataframes together horizontally, i.e. along axis = 1.
training_data_df = pd.concat([scaled_numeric_ind_df, encoded_categorical_ind_df], axis=1 )

print(training_data_df.head(5))


# ### Prepare Dependent Variable for Regression Analysis
# Encode the income variable which contain categoies like "<=50K" and ">50K" into numerical values inorder to carry out linear regression later.

# In[14]:


# Isolate the dependent variable from the original data frame, and
# Encode categorical data into numerical data
numeric_dependent_df = census_df['income'].replace({" <=50K": 0, " >50K": 1})


# #### Split independent variables (features) and dependent variables (target).
# Use  train_test_split to shuffel the data and split it into training and test subsets

# In[15]:


independent_train, independent_test, dependent_train, dependent_test = train_test_split(
    training_data_df, numeric_dependent_df, 
    test_size = 0.2, random_state = 42)


# This concludes the data preparation steps.
# Next Steps:
# Load each of the csv files in this folder into your coding space.
# - Use 'independent_train' and 'dependent_train' when training the model.
# - Use 'independent_test' and 'dependent_test' when evaluating the model.

# In[16]:


# Saving variables as CSV files to load onto models later.
independent_train.to_csv('independent_train.csv', index=False)
independent_test.to_csv('independent_test.csv', index=False)
dependent_train.to_csv('dependent_train.csv', index=False)
dependent_test.to_csv('dependent_test.csv', index=False)
