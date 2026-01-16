#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Here it starts, the heroic path of data hygiene. 

#Might be interesting to check who reports the shark attacks and when, so we could wonder where is the void in reports.
#The data may show that sharks only attack on english spoken territory.
#If data is accurate enough, it could be funny to search at what time does sharks set their tables.
#Might more attacks could be reported a couple years after Jaws was premiered? 1975 is the year.

#SEEMINGLY RELEVANT COLUMNS:
#Tier1: Country, Date, Time, Year 
#Tier2: Name, Area, Location, Activity

#Crucial facts:
#-In 2008 a polar bear jaw was found in a Greenland shark's stomach.
#-This is Mary Lee, the influencer shark: https://twitter.com/maryleeshark


# In[2]:


import pandas as pd
pd.set_option('display.max_columns', None)

import numpy as np

import warnings
warnings.filterwarnings('ignore')

#Hokusai level shark graphics.
import pylab as plt
import seaborn as sns

#This make the graph possible
get_ipython().run_line_magic('matplotlib', 'inline')

#Functions
from src.funk import Funk 


# In[3]:


#Load messy fishy data from attacks.csv

tibu = pd.read_csv('attacks.csv', encoding= "ISO-8859-1")


# In[4]:


tear1 = tibu[['Date', 'Year', 'Country','Time']]
tear2 = tibu[['Name', 'Area', 'Location', 'Activity']]
tears = tibu[['Date', 'Year', 'Country','Time', 'Name', 'Area', 'Location', 'Activity']]


# In[5]:


# Null values in columns. Remember that the important ones are as follow:
#Tier1: Country, Date, Time, Year 
#Tier2: Name, Area, Location, Activity
#Originally, the file has 25723 rows

'''nan_cols = tibu.isna().sum()

nan_cols[nan_cols>0]'''

#Irrelevant, as it comes, this file is mainly null values. Let's check it by row. But let's drop duplicates first.


# In[6]:


#Originally, the dataframe had (25723, 24) rows, after dropping duplicates it has (6312, 24)
tibu = tibu.drop_duplicates()


# In[7]:


Funk.check_nan(tibu)


# In[8]:


#Dropping all rows where Tear1 columns are null. Before (6312, 24), then (6302, 24)
tibu.drop(tibu[(tibu.Country.isna() == 1) & (tibu.Date.isna() == 1) 
                                           & (tibu.Time.isna() == 1)
                                           & (tibu.Year.isna() == 1)].index, axis=0, inplace=True)


# In[9]:


tibu.loc[(tibu.Country.isna() == 1) & (tibu.Time.isna() == 1) & (tibu.Name.isna() == 1)]


# In[ ]:





# In[ ]:





# In[10]:


#for searching nan values data.cylinders.iloc[[21506]]
#tibu.loc[(tibu.Date.isna() == 1)]
#tibu[['Country', 'Date', 'Time', 'Year']].head(40)


# In[11]:


tibu.reset_index(drop=True, inplace=True)


# In[12]:


tibu.to_csv('mar_de_caca.csv', index=False)


# In[ ]:



