#!usr/bin/env python
# -*- coding: utf-8 -*-

# Usage: python [files]
# Data Cleaning and Splitting    

import pandas as pd

# Get row summary, return a list of indices of rows that contains NaNs
def row_summary(df):
    # Extract column headers
    featNames = list(df.columns.get_values()) 
    # Get row summary (whether number of NaNs in each row)
    row_summary = df.isnull().sum(axis=1)
    # Get incomplete row indices 
    nan_row_inds = list() # incomplete row indices
    for i, x in enumerate(row_summary):
        if x > 0: nan_row_inds.append(i)
    return nan_row_inds

# Drop the incomplete records (has NaNs)
def clean_records(df):
    nan_row_inds = row_summary(df)
    clean_df = df.drop(df.index[nan_row_inds], inplace=False)    
    # Double check for NaNs 
    print 'Is there any NaNs in the clean records?', clean_df.isnull().values.any()
    return clean_df

# Separate data columns into a subgroup and save into a csv file
def split_data(df, start_ind, end_ind, csv_name): 
    subdf = df.iloc[:, [i for i in range(start_ind, end_ind)]]   
    # Add patient ID columns
    if start_ind > 0:
        key = df['Patient_ID'] # unique record key
        subdf = pd.concat([key, subdf], axis=1)
    subdf.to_csv(csv_name, index=False)
    return subdf

if __name__ == '__main__':
    df = pd.read_csv('Data_Adults_1.csv')
    clean_df = clean_records(df)
    n, m = clean_df.shape

    # Find the splitting point
    featNames = list(df.columns.get_values()) 
    split_point1 = featNames.index('BSC_Respondent')
    split_point2 = featNames.index('LDS_79')
    
    df1 = split_data(clean_df, 0, split_point1, 'patient_info.csv') # patient information
    df2 = split_data(clean_df, split_point1, split_point2+1, 'unknown.csv') # unknown measurements
    df3 = split_data(clean_df, split_point2+1, m, 'base_concen.csv') #  baseline-concentration 
    
    # Separate baseline and concentration columns
    base_heads, concen_heads = ['Patient_ID'], []
    for h in df3.columns:
        if 'Baseline' in h:
            base_heads.append(h) 
        else:
            concen_heads.append(h)

    df_base = df3[base_heads]
    df_base.to_csv('baseline.csv', index=False)
    df_concen = df3[concen_heads]
    df_concen.to_csv('concentration.csv', index=False)