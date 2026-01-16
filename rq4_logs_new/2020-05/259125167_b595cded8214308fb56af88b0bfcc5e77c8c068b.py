#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  1 13:45:04 2020

@author: Bella
"""

"""
1. (5 points) Describe the that you use to solve this problem. (Hint: get it from your report.)
2. (5 points) Import the Pandas package.
3. (5 points) Load data into separate Data Frames. (All of you have collected multiple files.
    Work with at least 2 files for this problem.)
    a. Use csv functions to load data from csv files if your data is in csv.
    b. Use json functions to load data from json documents if your data is in json.
4. (5 points) Check the data-type of each of column by outputting the dttypes attribute of
    your DataFrame.
5. (5 points) Show an example of sorting one of your DataFrames by a column. Give the
top-15 entries in descending order.
6. (10 point) Give an example of using filtering.
    a. Give an example for horizontal filtering/slicing where you select a subset of the
    columns. This corresponds to a projection in a SELECT statement.
    b. Give an example for vertical filtering/slicing where you select a subset of the
    rows in your DataFramer according to some criteria. This corresponds to WHERE
    in SELECT statement.
7. (10 points) Show an example where you merge two DataFrames.
8. (5 points) Export the merged DataFrame to
    a. A csv file if your input data is in json documents.
    b. A json file if your input data is in csv files.
"""

#2. (5 points) Import the Pandas package.
import pandas as pd



#3. (5 points) Load data into separate Data Frames.
or_df = pd.read_csv("data/OR_DataProjection.csv")

print(or_df)

#4. (5 points) Check the data-type of each of column by outputting the dttypes attribute of your DataFrame.
print(or_df.dtypes)

#5. (5 points) Show an example of sorting one of your DataFrames by a column. Give thetop-15 entries in descending order.
or_df_sorted_descending = or_df.sort_values(['cases'], ascending=False)

print(or_df_sorted_descending.head(15))


#3. (5 points) Load data into separate Data Frames.
wa_df = pd.read_csv("data/WA_DataProjection.csv")

print(wa_df)

#4. (5 points) Check the data-type of each of column by outputting the dttypes attribute of your DataFrame.
print(wa_df.dtypes)

#5. (5 points) Show an example of sorting one of your DataFrames by a column. Give the
#top-15 entries in descending order.
wa_df_sorted_descending = wa_df.sort_values(['cases'], ascending=False)

print(wa_df_sorted_descending.head(15))


#6. (10 point) Give an example of using filtering. 
#a. Give an example for horizontal filtering/slicing where you select a subset of the columns. This corresponds to a projection in a SELECT statement.
#b. Give an example for vertical filtering/slicing where you select a subset of the
#rows in your DataFramer according to some criteria. This corresponds to WHERE
#in SELECT statement.

#DESCENDING DATAFRAME
#horizontal filtering/slicing 
print(wa_df_sorted_descending.loc[:, 'date':'cases'])

print(or_df_sorted_descending.loc[:, 'date':'cases'])

#vertical filtering/slicing
print(wa_df_sorted_descending.loc[0:14, :])

print(or_df_sorted_descending.loc[0:19, :])

#ASCENDING DATAFRAME
#horizontal filtering/slicing 
print(wa_df.loc[:, 'date':'cases'])

print(or_df.loc[:, 'date':'cases'])

#vertical filtering/slicing
print(wa_df.loc[0:14, :])

print(or_df.loc[0:19, :])



#7. (10 points) Show an example where you merge two DataFrames.
#8. (5 points) Export the merged DataFrame to
#    a. A csv file if your input data is in json documents.
#    b. A json file if your input data is in csv files.

#Oregon Executive Order
or_ex = pd.read_csv("data/OR_executive_orders.csv")


print(or_ex.head(20))

or_df.set_index(pd.to_datetime(or_df['date']), inplace=True)
or_ex.set_index(pd.to_datetime(or_ex['Date']), inplace=True) 

OR_merged_df = pd.merge(or_ex, or_df, how='outer', left_index=True, right_index=True)
OR_merged_df = OR_merged_df.drop(columns='date')
OR_merged_df = OR_merged_df.drop(columns='Date')

OR_merged_df.columns.name = 'Date'

#8. (5 points) Export the merged DataFrame to
#    a. A csv file if your input data is in json documents.
#    b. A json file if your input data is in csv files.
OR_merged_df.to_json('data/OR_merged_json_table.json', orient = 'table') #table version 
OR_merged_df.to_json('data/OR_merged_json_records.json', orient = 'records')

OR_merged_df.to_csv("data/OR_merged_dataframe.csv")

print(OR_merged_df)

#Washington Executive Order
wa_ex = pd.read_csv("data/WA_executive_orders.csv")


print(wa_ex.head(20))

wa_df.set_index(pd.to_datetime(wa_df['date']), inplace=True)
wa_ex.set_index(pd.to_datetime(wa_ex['Date']), inplace=True) 

WA_merged_df = pd.merge(wa_ex, wa_df, how='outer', left_index=True, right_index=True)
WA_merged_df = WA_merged_df.drop(columns='date')
WA_merged_df = WA_merged_df.drop(columns='Date')

WA_merged_df.columns.name = 'Date'

print(WA_merged_df)

#8. (5 points) Export the merged DataFrame to
#    a. A csv file if your input data is in json documents.
#    b. A json file if your input data is in csv files.
WA_merged_df.to_json('data/WA_merged_json_table.json', orient= 'table')
WA_merged_df.to_json('data/WA_merged_json_records.json', orient = 'records')

WA_merged_df.to_csv("data/WA_merged_dataframe.csv")


