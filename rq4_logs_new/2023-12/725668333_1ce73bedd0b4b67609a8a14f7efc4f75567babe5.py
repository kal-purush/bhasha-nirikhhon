#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
print("                                         Ex1")
print("")
data = pd.Series([10, 20, 30, 40, 50])
print(data)
print("#" * 100)


# In[2]:


print("                                         Ex2")
print("")
data = pd.Series([10, 20, 30, 40, 50])
data_list = data.tolist()
data_type = type(data_list)

print("Series as a list:", data_list)
print("Type of the list:", data_type)
print("#" * 100)


# In[5]:


print("                                         Ex3")
print("")
series1 = pd.Series([1, 2, 3, 4, 5])
series2 = pd.Series([5, 4, 3, 2, 1])

comparison = series1 == series2
print(comparison)
print("#" * 100)


# In[11]:


print("                                         Ex4")
print("")
data = pd.Series([10, 20, 30, 40, 50])

data = data.astype(float)  
print(data)
print(type(data))
print("#" * 100)


# In[10]:


print("                                         Ex5")
print("")
data = pd.DataFrame({'X': [78, 85, 96, 80, 86],
                     'Y': [84, 94, 89, 83, 86],
                     'Z': [86, 97, 96, 72, 83]})

powers = data.pow([2, 2, 2])
print(powers)
print("#" * 100)


# In[16]:


import numpy as np
print("                                         Ex6")
print("")
exam_data = {
    'name': ['Anastasia', 'Dima', 'Katherine', 'James', 'Emily', 'Michael', 'Matthew', 'Laura', 'Kevin', 'Jonas'],
    'score': [12.5, 9, 16.5, np.nan, 9, 20, 14.5, np.nan, 8, 19],
    'attempts': [1, 3, 2, 3, 2, 3, 1, 1, 2, 1],
    'qualify': ['yes', 'no', 'yes', 'no', 'no', 'yes', 'yes', 'no', 'no', 'yes']
}

labels = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

df = pd.DataFrame(exam_data, index=labels)

first_three_rows = df.head(3)
print("First three rows of the data frame:")
print(first_three_rows)
print("#" * 100)


# In[19]:


print("                                         Ex7")
print("")
df = pd.DataFrame({'foo': ["x", "y", "Z"],
                   'bar': [6, 10, None],
                   'baz': [True, False, False]
                   })
s3 = pd.DataFrame({'num': [1, 2, 3, 4]})
x = pd.concat([df, s3], axis=1)
y = x.set_index('num')
print(y)
print("#" * 100)


# In[20]:


print("                                         Ex8")
print("")
df = pd.DataFrame({'foo': ["x", "y", "Z"],
                   'bar': [6, 10, None],
                   'baz': [True, False, False]
                   })
s3 = pd.DataFrame({'num': [1, 2, 3, 4]})
x = pd.concat([df, s3], axis=1)
y = x.set_index('num')
y.reset_index(inplace=True)
print(y)
print("#" * 100)


# In[25]:


print("                                         Ex9")
print("")
data_range = pd.date_range(start='1/1/2023', end='1/12/2023', freq='H')
df = pd.DataFrame(data_range, columns=['date'])
df['data'] = range(len(data_range))
print(df)
print("#" * 100)


# In[5]:


print("                                         Ex10")
print("")
data = {'col1': [1, 2, 3, 4, 5],
        'col2': ['A', 'B', 'C', 'D', 'E'],
        'col3': [10, 20, 30, 40, 50]}
df = pd.DataFrame(data)
y = df.set_axis(['Serial', 'Letter', "Numbers"], axis='columns')
print(y)
print("#" * 100)


# In[8]:


print("                                         Ex11")
print("")
data = pd.Series([10, 20, 30, 40, 50])
row = data[2]
print(f"Selected Row: {row}")
print("#" * 100)


# In[ ]:



