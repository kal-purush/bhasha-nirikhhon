
# coding: utf-8

# <h1>Broadcasting</h1>

# In[1]:


import numpy as np


# In[2]:


my_3D_array = np.arange(70)
my_3D_array.shape = (2,7,5)
my_3D_array


# In[3]:


# shape 
my_3D_array.shape


# In[4]:


# number of dimensions 
my_3D_array.ndim


# In[5]:


# size; number of elements 
my_3D_array.size


# In[6]:


# data type for each element 
my_3D_array.dtype


# In[7]:


5 * my_3D_array -2 


# In[9]:


left_mat = np.arange(6).reshape(2,3)
right_mat = np.arange(15).reshape(3,5)


# In[10]:


# 2x3 mult 3x5 should = 2x5 but throwing error, inner does 1D, use dot for 2D 
np.inner(left_mat, right_mat)


# In[11]:


np.dot(left_mat, right_mat)


# In[12]:


my_3D_array


# In[13]:


my_3D_array.shape


# In[14]:


my_3D_array.sum()


# In[15]:


(69*70)/2


# In[16]:


my_3D_array.sum(axis=0)


# In[17]:


my_3D_array.sum(axis=1)


# In[18]:


my_3D_array.sum(axis=2)


# ### Broadcasting Rules

# In[20]:


array_2D = np.ones(35, dtype='int').reshape(7,5)*3
array_2D


# In[23]:


rand_2D = np.random.random((7,5))
rand_2D


# In[24]:


np.set_printoptions(precision=4)
my_3D_array * rand_2D


# In[25]:


my_vec = np.arange(5)*7
my_vec[0] = -1
my_vec


# In[26]:


my_3D_array / my_vec


# In[27]:


my_3D_array % my_vec


# In[ ]:



