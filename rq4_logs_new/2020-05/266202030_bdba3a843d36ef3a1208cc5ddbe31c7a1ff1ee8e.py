#!/usr/bin/env python
# coding: utf-8

# In[18]:


import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")
    
# Python Libraries

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA 

# plotly visualization library
import plotly
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.express as px
from plotly import tools
from scipy import interp

init_notebook_mode(connected=False)


# In[22]:


df.info()


# In[23]:


df.isnull().sum()


# In[30]:


df["group"].value_counts()


# In[25]:


df["group_label"].value_counts()


# In[26]:


df["target_label"].value_counts()


# In[19]:


#Wrangling Data
class DataWrangle:
    def __init__(self):# Extracting data for the model
        self.input_path = "dataset/MovementAAL_RSS_"
        self.target_path = "dataset/MovementAAL_target.csv"
        self.group_path = "groups/MovementAAL_DatasetGroup.csv"

    def load_data(self):# Loading 315 individual files and making small dataframes

        self.df_lst = []

        for i in range(1, 315):
            self.file_path = self.input_path + str(i) + ".csv"
            self.file_df = pd.read_csv(self.file_path, header=0).reset_index(drop=True)
            self.file_df["_id"] = i
            self.file_df = self.file_df.reset_index(drop=True)
            self.df_lst.append(self.file_df)

        # Constructing the final input dataframe by concatenating the small dataframes
        self.input_df = pd.concat(self.df_lst, sort=True)
        self.target_df = pd.read_csv(self.target_path)[" class_label"]
        self.group_df = pd.read_csv(self.group_path)[" dataset_ID"]

    def add_target_class_and_time(self):

        # Adding target classes to the input dataframe
        self.group_lst = []
        # Adding time to the dataset at 8Hz sampling frequency
        for idx, (id_num, target, group) in enumerate(
            zip(self.input_df["_id"].unique(), self.target_df, self.group_df)
        ):
            self.gr = self.input_df[self.input_df["_id"] == id_num]
            self.gr = self.gr.reset_index(drop=True)
            self.gr["target"] = [target] * self.gr.shape[0]
            self.gr["group"] = [group] * self.gr.shape[0]
            self.gr["time"] = np.arange(0, self.gr.shape[0] / 8, 1 / 8)
            self.group_lst.append(self.gr)

        # Constructing the final dataframe by concatenating all the group dataframes
        self.df = pd.concat(self.group_lst)
        self.df = self.df.reset_index(drop=True)

        # Adding target label and group label - Movement and Non-Movement
        self.df["target_label"] = self.df["target"].apply(
            lambda x: "Movement" if x == 1 else "Non-Movement"
        )
        self.df["group_label"] = np.select(
            condlist=[self.df["group"] == 1,self.df["group"] == 2,self.df["group"] == 3,], 
            #Alloting datapoints to three envionments 
            choicelist=["environment_1", "environment_2", "environment_3"],
        )
    #Renaming and rearranging columns for better processing and understanding
    def rename_and_rearrange_columns(self):
        self.df = self.df.rename(
            columns={
                "#RSS_anchor1": "RSS_anchor1"," RSS_anchor2": "RSS_anchor2"," RSS_anchor3": "RSS_anchor3",
                " RSS_anchor4": "RSS_anchor4",
            }
        )

        self.df = self.df[
            [
                "_id","time","RSS_anchor1","RSS_anchor2","RSS_anchor3","RSS_anchor4","target","target_label",
                "group","group_label",
            ]
        ]

        # saving the preprocessed file
        self.df.to_csv("indoor_movement.csv", index=False)
        return self.df


if __name__ == "__main__":
    wrangle = DataWrangle()
    wrangle.load_data()
    wrangle.add_target_class_and_time()
    df = wrangle.rename_and_rearrange_columns()


# In[20]:


# Modified Dataset
df.head()


# In[4]:


#Time Series Visualization
class TimeseriesViz:
    def __init__(self, df):
         self.df_red = df[df["_id"].isin(np.arange(1, 315))]

    def ts_viz(self):
        # time series visualization (x_axis: time, y_axis: sensor output)
        for i in range(1, 5):
            self.fig = px.line(
                self.df_red,x="time",y="RSS_anchor{}".format(i),color="target_label",line_group="_id",
                hover_name="target",line_shape="linear",width=950,height=550,line_dash="_id",
                facet_row="target_label",animation_frame="group_label",
                color_discrete_sequence=[
                    px.colors.qualitative.Vivid[0],
                    px.colors.qualitative.Vivid[1],
                ],
                template="seaborn",
            )
            iplot(self.fig, filename="timeseries_{i}")

if __name__ == "__main__":

    viz = TimeseriesViz(df)
    viz.ts_viz()


# In the above plot, each different type of dotted and undotted line represents a different path. It can be observed that there are 6 types of lines each representing a path present in the dataset. The orange line represents movement and blue represent non-movement in the plot in case of each environment.

# In[6]:


# Histogram Visualization
class HistogramViz:
    def __init__(self, df):
        self.df = df

    def histogram_viz(self):
        # Signal visualization using histogram
        for i in range(1, 5):
            self.fig = px.histogram(
                self.df,x="RSS_anchor{}".format(i),y="RSS_anchor{}".format(i),color="target_label",
                facet_col="target_label",barmode="group",nbins=70,height=400,width=970,
                color_discrete_sequence=[
                    px.colors.qualitative.Vivid[0],
                    px.colors.qualitative.Vivid[1],
                ],
                template="ggplot2",
            )
            iplot(self.fig, filename="histogram_{i}")


if __name__ == "__main__":
    hist = HistogramViz(df)
    hist.histogram_viz()


# The histogram clearly visualizes the signal power and count of RSS of each sensor. This is further divided into 
# movement and non-movement type.

# In[27]:


# Dimension Reduction and Emphasize variation using Principal Component Analysis
class DimensionDecomp:
    def __init__(self, df):
        self.input_cols = df[
            ["RSS_anchor1", "RSS_anchor2", "RSS_anchor3", "RSS_anchor4"]
        ]
        self.target_col = df[["target_label"]]
        self.target_group = df[["group_label"]]
        
#Principal Component Analysis: Principal Component Analysis is a method which is used to reduce dimensionality 
#of data computing eigenvalues and eigenvectors of matrix data.

    def pca_comp(self): #Principal Component Analysis
        self.pca = PCA(n_components=2, random_state=42)
        self.pca_res = self.pca.fit_transform(self.input_cols)
        self.pca_res = pd.DataFrame(self.pca_res, columns=["pc1", "pc2"])
        self.pca_res = pd.concat(
            [self.pca_res, self.target_col, self.target_group], axis=1, sort=False
        )
        self.pca_res = self.pca_res.sample(n=1000, random_state=42)
        self.pca_res = self.pca_res.sort_values(by=["group_label"])

        return self.pca_res

    def pca_project(self, pca_res):#PCA projection

        self.fig = px.scatter(
            pca_res,
            x="pc1",
            y="pc2",
            color="target_label",
            hover_name="target_label",
            width=970,
            height=500,
            color_discrete_sequence=px.colors.qualitative.Vivid,
            animation_frame="group_label",
            template="seaborn",
        )
        iplot(self.fig)


# In[28]:


# plotting PCA projection for different groups
if __name__ == "__main__":

    decomp = DimensionDecomp(df)
    pca_res = decomp.pca_comp()
    decomp.pca_project(pca_res)


# In[41]:


new_df = df[df.columns.difference(["group_label","target_label"])]
new_df.head()


# In[42]:


corr = new_df.corr()
corr.style.background_gradient(cmap='coolwarm')


# In[48]:


df["_id"].value_counts()


# In[ ]:



