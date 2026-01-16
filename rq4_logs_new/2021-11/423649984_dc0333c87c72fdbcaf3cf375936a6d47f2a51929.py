#!/usr/bin/env python
# coding: utf-8

# # Dictionaries
# 
# Python dictionaries can be seen as an extension of lists. Recall list syntax:
# 
# ### ```[item1, item2, item3, ...]```<br>
# 
# To access an item in a list, you could reach a value by its index. Dictionaries, however, involve <b>key-value</b> pairs. That means a dictionary looks more like this:
# 
# ### ```{"key1":"value1", "key2":"value2",...}```<br>
# 
# **Contents:**
# 
# a. <a href="#Define-a-dictionary">Define a dictionary</a><br>
# b. <a href="#Get-Value-of-a-Key">Get Value of a Key</a><br>
# c. <a href="#Access-Keys-In-Dictionary">Access Keys In Dictionary</a><br>
# d. <a href="#Access-Values-In-Dictionary">Access Values In Dictionary</a><br>
# e. <a href="#Update-a-value-in-a-dictionary">Update a value in a Dictionary</a><br>
# f. <a href="#Delete-a-value-from-a-dictionary">Delete a value from a Dictionary</a><br>
# g.  <a href="#Add-a-value-to-a-dictionary">Add a value to a Dictionary</a><br>
# h. <a href="#Dictionary-Comprehension">Dictionary Comprehension</a><br>
# i. <a href="#Nested-dictionary">Nested dictionary</a><br>
# 

# **Motivating example:**
# 
# A good way to initially think about a dictionary is that the **<font color = 47638d>keys are like the indices of a list</font>**, while the values are the actual elements of a list. Consider the list below. Notice that the indices are numbers like we're used to.  
# 
# <img src=office_list3.png height=1000 width=1000>
# 

# And recall that we access the elements in a list using the corresponding index as below:

# In[4]:


office_list = ["Regional Manager", "Assistant TO the Regional Manager", "Salesman", "Receptionist", "Lead HR Representative"]
office_list[3]


# <br><br>However, what if we made this more meaning full and **<font color=47638d>gave each index a name</font>**?
# 
# <img src=office_dict.png width=1000 height=1000>
# <br>
# Now we can use the "names" just like an index for access!

# In[1]:


office_dict = {
    "Michael": "Regional Manager",
    "Dwight": "Assistant TO the Regional Manager",
    "Jim": "Salesman",
    "Pam": "Receptionist",
    "Toby": "Lead HR Representative"
}
office_dict['Pam']


# Notice how this is more intuitive than using lists as well.
# 
# Note that some similarities end here. For example, you can't slice a dictionary.

# ### Define the dictionary
# 
# Use squiggly brackets and separate key-values pairs with a colon. We separate wach key-value pair with a comma. For this example, we'll use characters from The Office:

# In[26]:


office_dict = {
    "Michael": "Regional Manager",
    "Dwight": "Assistant TO the Regional Manager",
    "Jim": "Salesman",
    "Pam": "Receptionist",
    "Toby": "Lead HR Representative"
}


# ### Get Value of a Key
# 
# Just like how we would access an element of a list with a index, we access a value by providing the key.

# In[6]:


office_dict['Michael']


# **What if the key doesn't exist?**<br>
# <font color = gray>We may set a default value for these, if we anticipate that a key might not exist.<font>

# In[39]:


office_dict.get("Bob Vance, Vance Refrigeration", "Not a main character!")


# ### Access Keys In Dictionary

# In[9]:


office_dict.keys()


# **We can access keys by iterating through them as well**

# In[8]:


for key in office_dict.keys():
    print(key)


# ### Access Values In Dictionary

# In[11]:


office_dict.values()


# ### Access Keys and Values In Dictionary
# 
# Access all of your all keys and values in key-value tuple pairs:

# In[12]:


office_dict.items()


# ### Update a value in a dictionary
# 
# You can update the job title of an employee when they get a **<font color=gold>promotion</font>** as follows:

# In[13]:


office_dict['Dwight'] = "Assistant Manager"


# ### Delete a value from a dictionary
# 
# You can remove key-value pairs from a dictionary too:

# In[27]:


# Casino Nights happens...
del office_dict['Jim']


# In[28]:


office_dict


# ### Add a value to a dictionary

# In[16]:


office_dict['Andy'] = "Sales Representative"


# ### Dictionary Comprehension
# 
# The syntax is:
# 
# ### ```{key:value for key, value in [list of lists]}```<br>
# 
# Simple example

# In[17]:


nums = [1, 2, 3, 4, 5, 6]
{i:i**2 for i in nums}


# We can use two lists and the  ```zip``` function to make a dictionary comprehension too. Syntax:
# 
# ### ```{key:value for key, value in zip(list1, list2)}```

# In[18]:


stamford = ["Karen", "Martin", "Tony"]
stamford_salary = [50000, 45000, 40000]
{name:salary for name, salary in zip(stamford, stamford_salary)}


# In[2]:


#In this example, we'll take their yearly salaries and divide by 26 to get their bi-weekly pay:
office_yearly_salary_dict = {
    "Michael": 100000,
    "Dwight": 50000,
    "Jim": 45000,
    "Pam": 25000,
    "Toby": 60000
}
office_biweekly_salary_dict = {name:round(salary/26, 2) for name, salary in office_yearly_salary_dict.items()}
office_biweekly_salary_dict


# ### Sorting a Dictionary
# 
# Using .items() to get a list of key-value tuple pairs, we can then use this to sort a dictionary.

# In[20]:


office_yearly_salary_dict.items()


# In[21]:


office_yearly_salary_srt = sorted(office_yearly_salary_dict.items(),
                                  key = lambda x: x[1]) # we sorted by the second element in each tuple, which was the salary
office_yearly_salary_srt


# We can now transform the list of tuples in a dictionary.
# 
# The ```dict()``` function naturally knows to make a list of tuple pairs into a dictionary.

# In[22]:


dict(office_yearly_salary_srt)


# ## Merging a number of dictionaries

# In[40]:


# intialize some dictionaries for our example
# note that if there are two taht are equal, the right key that's in the right-most dict will win
accountant_salaries = {"Kevin": 15000, "Oscar": 45000, "Angela": 50000}
other_salaries = {"Creed": 0, "Meredith": 25000, "Kelly": 25000}
warehouse_salaries = {"Darryl": 40000, "Roy":23000, "Pudge": 23000}


# In[30]:


{**accountant_salaries, **other_salaries, **warehouse_salaries}


# ### Nested dictionary
# 
# Dictionaries with more data than just key-value pairs can be created through nesting (i.e. having dictionaries inside of each other).
# 
# In this example, we'll have both job title and yearly salary included for a character, instead of having them be separately defined dictionaries:

# In[33]:


office_nested_dict = office_dict = {
    
    "Michael": {
        "title": "Regional Manager", 
        "salary": 100000
    },
    
    "Dwight": {
        "title": "Assistant Regional Manager", 
        "salary": 50000
    },

    "Jim": {
        "title": "Sales Representative", 
        "salary": 45000
    },

    "Pam": {
        "title": "Receptionist", 
        "salary": 25000
    },

    "Toby": {
        "title": "HR Representative", 
        "salary": 60000
    }
}


# **Printing a nested dictionary in a more readable format**

# In[34]:


# this is the default way that dictionaries are printed
office_nested_dict


# In[37]:


import json

# more readable way to print the dictionary
print(json.dumps(office_nested_dict, indent = 4))


# ## Default Dict (bonus)
# 
# if we try to get reference that's not there, we can set a default value for it
# 
# 
# For example counting how many times a word appear in a script.
# 

# In[3]:


script = "I’m an early bird and I’m a night owl so I’m wise and I have worms"
script_list = script.split()


# In[4]:


from collections import defaultdict

word_counts = defaultdict(lambda:0) # must be function-like

for word in script_list:
    
    word_counts[word] += 1


# In[5]:


word_counts


# **A more complex example**<br>
# 
# We find the position(s) of each word in the script.

# In[6]:


word_positions = defaultdict(list) # must function-like
for ix, word in enumerate(script_list):
    word_positions[word].append(ix)


# In[7]:


word_positions
