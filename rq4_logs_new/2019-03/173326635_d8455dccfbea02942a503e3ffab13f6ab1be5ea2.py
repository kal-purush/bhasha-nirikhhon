
# coding: utf-8

# In[ ]:


import pandas as pd


# In[ ]:


from process import preprocess


# Get dataset with features

# In[ ]:


config = {
    'pred_var': 'Torvet PM10', # Must include station and pollutants name (column name)
    'stations': ['Torvet'], # Stations to use in feature extraction
    'window': 6,
}

data = preprocess(**config)


# In[ ]:


print('X train', data['X_train'].shape)
print('y train', data['y_train'].shape)
print('X validation', data['X_val'].shape)
print('X test', data['X_test'].shape)

#print(data['X_train'].columns)


# **Train Multi Output RF | GBM | MLP**
# 
# _Params are hidden inside each file_

# In[ ]:


import GBM as GBM
GBM.train(config, data)
gbm_results, rmse, r2 = GBM.predict(config, data)
print('RMSE: {:.2f}   R2:{:.2f}'.format(rmse, r2))


# In[ ]:


import RF as RF
RF.train(config, data)
rf_results, rmse, r2 = RF.predict(config, data)
print('RMSE: {:.2f}   R2:{:.2f}'.format(rmse, r2))


# In[ ]:


import MLP as MLP
MLP.train(config, data)
mlp_results, rmse, r2 = MLP.predict(config, data)
print('RMSE: {:.2f}   R2:{:.2f}'.format(rmse, r2))


# **Plotting the results**

# In[ ]:


import plotly
import plotly.graph_objs as go
from plotly.offline import iplot, init_notebook_mode
import cufflinks

plotly.tools.set_credentials_file(username='evenmm', api_key='4iqRJUmS1Hhuno44DAl7')


# In[ ]:


gbm_results['RF'] = rf_results['RF']
gbm_results['MLP'] = mlp_results['MLP']
#gbm_results['2019-02-15':]
#iplot(gbm_results['2019-02-15':], image='png')
#iplot(gbm_results['2019-02-15':])
#gbm_results['2019-02-15':].iplot()

