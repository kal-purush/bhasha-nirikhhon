# -*- coding: utf-8 -*-
"""
Created on Mon Sep 27 10:46:22 2021

@author: Colby
"""

import pandas as pd
import numpy as np
import seaborn as sb
import matplotlib.pyplot as plt

import datetime



data_file_path = 'C:/Eden/aqi/subset_aqi_az.csv'

raw_data_df = pd.read_csv(data_file_path)

raw_data_df["Date"] = pd.to_datetime(raw_data_df["Date"])


tmp_df = raw_data_df.loc[raw_data_df["county Name"]=="Maricopa",["Date","AQI"]].sort_values(by=["Date"])


plt.plot(tmp_df["Date"], tmp_df["AQI"])