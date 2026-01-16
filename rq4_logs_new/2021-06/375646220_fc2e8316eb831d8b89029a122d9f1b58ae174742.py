# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 11:13:17 2021

@author: Vadym
"""

import os
import pandas as pd

os.chdir("data")

resources_file = "资源信息.csv"
resources_df = pd.read_csv(resources_file)
assets_file ='资产信息.csv'
assets_df = pd.read_csv(assets_file)
assets_df = assets_df.rename(columns={"ASSETSNAME":"NAME","ASSETID":"ID"})
resources_df = resources_df.rename(columns={"RESOURCENAME":"NAME","RESOURCEID":"ID"})
resources_columns = resources_df.columns.tolist()
assets_columns = assets_df.columns.tolist()
all_columns = resources_columns + assets_columns
common_columns = []
for col in all_columns:
    if col in resources_columns and col in assets_columns:
        common_columns.append(col)
common_columns = set(common_columns)
print(common_columns) 
filtered_resources_df = resources_df.filter(common_columns, axis=1)
filtered_assets_df = assets_df.filter(common_columns, axis=1)
res_df = filtered_assets_df.append(filtered_resources_df, ignore_index=True)
res_df.to_csv("assets_resources.csv")           

#print(res_df)