#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sklearn.base import BaseEstimator, TransformerMixin

import re


# In[2]:


class TextPreprocessor(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return [re.sub(r'[^a-zA-Z\s]', '', text.upper().strip()).strip() for text in X]
