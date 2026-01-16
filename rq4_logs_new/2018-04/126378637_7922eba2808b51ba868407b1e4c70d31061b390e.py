
# coding: utf-8

# In[1]:


## Uplaod the datafiles


# In[2]:


get_ipython().magic(u'matplotlib inline')
import numpy as np
import pandas as pd
dfschz=pd.read_csv('/home/nbuser/library/pgc.cross.SCZ17.2013-05.csv')
dfadd=pd.read_csv('/home/nbuser/library/pgc.cross.ADD4.2013-05.csv')
dfaut=pd.read_csv('/home/nbuser/library/pgc.cross.AUT8.2013-05.csv')
dfbip=pd.read_csv('/home/nbuser/library/pgc.cross.BIP11.2013-05.csv')
dfmdd=pd.read_csv('/home/nbuser/library/pgc.cross.MDD9.2013-05.csv')
dfschz.head(5)


# In[3]:


## create a scatterplot of odds-ratio


# In[4]:



dfschz['log_or'] = np.log10(dfschz['or'])
dfschz.plot.scatter(y='pval',x='log_or',s=0.1,title='Schz')

dfadd['log_or'] = np.log10(dfadd['or'])
dfadd.plot.scatter(y='pval',x='log_or',s=0.1,title='ADD')

dfaut['log_or'] = np.log10(dfaut['or'])
dfaut.plot.scatter(y='pval',x='log_or',s=0.1,title='Autism')

dfbip['log_or'] = np.log10(dfbip['or'])
dfbip.plot.scatter(y='pval',x='log_or',s=0.1,title='Bipolar')

dfmdd['log_or'] = np.log10(dfmdd['or'])
dfmdd.plot.scatter(y='pval',x='log_or',s=0.1,title='Manic Depression')


# In[5]:


#%matplotlib inline
#import matplotlib.pyplot as plt
#plt.style.use('seaborn-white')
#import numpy as np
#import astropy

#wcs = astropy.wcs.WCS()
#fig = matplotlib.pyplot.figure()
#fig = plt.figure()

#fig.subplots_adjust(hspace=0.4, wspace=0.4)
#schzfig =fig.add_subplot(2, 3, 1)
#addfig = fig.add_subplot(2, 3, 2)
#autfig = fig.add_subplot(2, 3, 3)
#bipfig = fig.add_subplot(2, 3, 4)
#mddfig = fig.add_subplot(2, 3, 5)


# In[6]:


## PCA: Remove data p < 0.05 and odds ratio <= 1


# In[7]:


dfschz_extreme=dfschz[(abs(dfschz['log_or'])>1.0) & (dfschz['pval']<0.05)]
dfschz_extreme.plot.scatter(y='pval',x='log_or',s=3, title='Schz')

dfadd_extreme=dfadd[(abs(dfadd['log_or'])>1.0) & (dfadd['pval']<0.05)]
dfadd_extreme.plot.scatter(y='pval',x='log_or',s=3,title='ADD')

dfaut_extreme=dfaut[(abs(dfaut['log_or'])>1.0) & (dfaut['pval']<0.05)]
dfaut_extreme.plot.scatter(y='pval',x='log_or',s=3,title='Autism')

dfbip_extreme=dfbip[(abs(dfbip['log_or'])>1.0) & (dfbip['pval']<0.05)]
dfbip_extreme.plot.scatter(y='pval',x='log_or',s=3,title='Bipolar')

dfmdd_extreme=dfmdd[(abs(dfmdd['log_or'])>1.0) & (dfmdd['pval']<0.05)]
dfmdd_extreme.plot.scatter(y='pval',x='log_or',s=3,title='Manic Depresssion')


# In[8]:


## Sort the SNPs by odds ratio


# In[9]:


dfschz_extreme = dfschz_extreme.sort_values(['log_or','pval'],ascending=[False, True])
dfadd_extreme = dfadd_extreme.sort_values(['log_or','pval'],ascending=[False, True])
dfaut_extreme = dfaut_extreme.sort_values(['log_or','pval'],ascending=[False, True])
dfbip_extreme = dfbip_extreme.sort_values(['log_or','pval'],ascending=[False, True])
dfmdd_extreme = dfmdd_extreme.sort_values(['log_or','pval'],ascending=[False, True])


# In[10]:


## Take the two top-most (indicative SNPs) and two bottom-most (protective SNPs)


# In[11]:


snps_smry = dfschz_extreme.head(2)
snps_smry = snps_smry.append(dfschz_extreme.tail(2))

snps_smry = snps_smry.append(dfadd_extreme.head(2))
snps_smry = snps_smry.append(dfadd_extreme.tail(2))

snps_smry = snps_smry.append(dfaut_extreme.head(2))
snps_smry = snps_smry.append(dfaut_extreme.tail(2))

snps_smry = snps_smry.append(dfbip_extreme.head(2))
snps_smry = snps_smry.append(dfbip_extreme.tail(2))

snps_smry = snps_smry.append(dfmdd_extreme.head(2))
snps_smry = snps_smry.append(dfmdd_extreme.tail(2))

snps_smry[['snpid','log_or','pval']]


# In[12]:


##Combine the 5 different files to create single df for SNPs of Interest


# In[13]:


dfsnps_oI = dfschz_extreme.append(dfadd_extreme)
dfsnps_oI = dfsnps_oI.append(dfaut_extreme) 
dfsnps_oI = dfsnps_oI.append(dfbip_extreme)
dfsnps_oI = dfsnps_oI.append(dfmdd_extreme)


# In[14]:


## search for SNPs that occur mutliple times


# In[15]:


from collections import Counter
counts = Counter(dfsnps_oI)
countsnps_oI = [value for value, count in counts.items() if count > 1]
countsnps_oI


# In[16]:


##sort for most potent SNPs (absolute greatest odds-ratios and lowest pvalues)


# In[33]:


dfsnps_oI['abs_log_or'] = abs(dfsnps_oI['log_or'])
                              
dfsnps_oI = dfsnps_oI.sort_values(['abs_log_or','pval'],ascending=[False, True])
dfsnps_oI[['snpid','log_or','pval']].head(20)


# In[18]:


get_ipython().magic(u'matplotlib inline')
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression,Lasso
from sklearn.datasets import make_regression, load_boston
from sklearn.model_selection import cross_val_score
from statsmodels.api import OLS
from scipy.stats import pearsonr,spearmanr


# In[19]:


##some code I found to One Hot Encode with scikit-learn


# In[20]:


from numpy import array
from numpy import argmax
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder

# define input
data = dfsnps_oI['a1']
values = array(data)

# integer encode
label_encoder = LabelEncoder()
integer_encoded = label_encoder.fit_transform(values)
dfsnps_OHE = dfsnps_oI
dfsnps_OHE['a1'] = integer_encoded

# define input
data = dfsnps_oI['a2']
values = array(data)

# integer encode
label_encoder = LabelEncoder()
integer_encoded = label_encoder.fit_transform(values)
dfsnps_OHE = dfsnps_oI
dfsnps_OHE['a2'] = integer_encoded


dfsnps_OHE.head()


# In[21]:


##using Dr. Gerkin's code to score our RTR and Linear regression


# In[22]:


##we're looking to find connections between the odds-ratio and other elements of our data


# In[23]:


X=dfsnps_OHE[['hg18chr','a1','a2']]
y=dfsnps_OHE['or']


# In[24]:


rfr = RandomForestRegressor(n_estimators=25) # This will make 25 decision trees and average them together
lm = LinearRegression()


# In[25]:


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression,Lasso
from sklearn.model_selection import cross_val_score, cross_val_predict,ShuffleSplit
from scipy.stats import pearsonr


# In[29]:


def scorer(est,X,y):
    """Computes the Pearson correlation between predicted and observed values"""
    predicted = est.predict(X).squeeze() # Get a prediction from the model, and reduce it to a 1-D array.  
    actual = y.squeeze() # Reduce the actual values to a 1-D array as well.  
    if predicted.var()==0: # If the variance of the prediction (across samples is zero)
        r = 0 # Then correlation is undefined, but could also be considered to be 0.  
    else:
        r,p = pearsonr(predicted,actual) # Compute the correlation between the prediction and the actual values
    return r


# In[30]:


# Linear regression
cross_val_score(lm,X,y,scoring=scorer, cv=4) 


# In[31]:


# random forest
cross_val_score(rfr,X,y,scoring=scorer, cv=4) 


# In[32]:


##there is no corrolation between the chromosome or alelle bases to the odds ratio
## but the random forest regression does perform better


# In[ ]:


## Does LASSO help?


# In[26]:


n_folds = y.shape[0]
def prediction_quality(est,X,y):
    """Computes the Pearson correlation between predicted and observed values"""
    ss = ShuffleSplit(0.5)
    predicted = cross_val_predict(est,X.values,y.values,cv=n_folds).squeeze()
    actual = y.values.squeeze()
    r,p = pearsonr(predicted,actual)
    se = (1-r**2)/np.sqrt(len(predicted)-2) # formula for standard error of correlation coefficient 
    return r,se


# In[27]:


# Initialize the classification algorithm (Random Forest Classification)
lm = Lasso(alpha=0.1, normalize=True)
print("R = %.3f +/- %.3f" % prediction_quality(lm,X,y))


# In[28]:


# Initialize the classification algorithm (Random Forest Classification)
rfr = RandomForestRegressor(n_estimators=100)
print("R = %.3f +/- %.3f" % prediction_quality(rfr,X,y))


# In[ ]:


## no
