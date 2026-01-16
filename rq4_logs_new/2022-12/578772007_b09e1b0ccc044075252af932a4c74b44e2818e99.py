import streamlit as st
import json
import requests
import pickle
import pandas as pd 
import numpy as np
import plotly.express as px

#----------------------------Import file---------------------------------#
df = pickle.load(open("dataset/df_for_deployment.pkl","rb"))
df = pd.DataFrame(df)
mapbox_access_token =  'pk.eyJ1Ijoiam9lbGpydCIsImEiOiJjbGJqaW12enYxM3M3M3hvZ2M1dzk3MThxIn0.xk3qkRogSnlc-wml2x63kQ'
px.set_mapbox_access_token(mapbox_access_token)
fig = px.scatter_mapbox(df, 
                        lat="latitude", 
                        lon="longitude",     
                        color="zone_code", 
                        size="count",
                        hover_name = "school",
                        color_continuous_scale=px.colors.cyclical.IceFire,
                        mapbox_style="dark",
                        size_max=21, 
                        zoom=10.3,
                       width=1000, height=600)

#----------------------------Functions---------------------------------#
def prayer_count(number,school):
    original_count = df.loc[school,"count"]
    original_count = original_count + number
    df.loc[school.upper(),['count']] = original_count
    return  

def display_map():
    fig = px.scatter_mapbox(df, 
                        lat="latitude", 
                        lon="longitude",     
                        color="zone_code", 
                        size="count",
                        hover_name = "school",
                        color_continuous_scale=px.colors.cyclical.IceFire,
                        mapbox_style="dark",
                        size_max=21, 
                        zoom=10.3,
                       width=1000, height=600)
    return st.plotly_chart(fig)
    
#----------------------------code---------------------------------#
st.title("Map Of Revival #HOGC21:fire::fire::fire:")

plot_spot = st.empty()
school = st.selectbox('Select school being prayed for revival',(df["school"]))
number = st.number_input(label = 'Number of people', step = 1, min_value = 1)
if st.button('Update revival map'):
    prayer_count(number,school)
    fig = px.scatter_mapbox(df, 
                        lat="latitude", 
                        lon="longitude",     
                        color="zone_code", 
                        size="count",
                        hover_name = "school",
                        color_continuous_scale=px.colors.cyclical.IceFire,
                        mapbox_style="dark",
                        size_max=21, 
                        zoom=10.3,
                       width=1000, height=600)
    

#fig.update_traces(cluster=dict(enabled=True))
with plot_spot:
    st.plotly_chart(fig)