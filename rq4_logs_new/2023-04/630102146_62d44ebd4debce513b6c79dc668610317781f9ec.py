#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import datetime
import numpy as np

import warnings
warnings.filterwarnings("ignore")


def prepare_store_items(df):
    '''
    prepare_store_items takes in store items df, converts date column to datetype
    dtype, sets it as the index, adds comuns for month, day of the week, and sales 
    total. 
    '''
    # set col as datetime
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    # set col as index
    df = df.set_index('sale_date').sort_index()
    # create month column
    df['month'] = df.index.month_name()
    # create day of week column
    df['day_of_week'] = df.index.day_name()
    # create sales total column
    df['sales_total'] = df['sale_amount'] * df['item_price']
    # return reconstructed df
    return df

def prep_ops_data(df):
    '''
    prep_ops_data takes in germany_ops_data df, converts Date col to datetime,
    sets it as the index, adds col for month and a col for year, the fills null
    values, renames columns to remove capitalization. 
    '''
    # convert date dtype to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    # set it as the index
    df = df.set_index('Date').sort_index()
    # rename columns to remove capitalization
    df = df.rename(columns={'Consumption':'consumption', 'Wind':'wind', 'Solar':'solar','Wind+Solar':'wind_solar'})
    
    # create col for month
    df['month'] = df.index.month_name()
    # create col for year
    df['year'] = df.index.year
    #fill nulls with 0
    df = df.fillna(0)
    
    return df
