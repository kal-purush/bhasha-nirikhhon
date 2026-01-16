import streamlit as st
import numpy as np
import pandas as pd
import pickle 


pipe=pickle.load(open("rf.pkl","rb"))
df = pd.read_pickle("df.pkl")

st.title("Laptop Predictor")
#company
company=st.selectbox("Brand",options=df["Company"].unique())
#TypeName
type=st.selectbox("Tyep",options=df["TypeName"].unique())
#Ram
Ram=st.selectbox("Ram(In GB)",options=[2,4,6,8,12,16,24,32,64])
#weight
weight=st.number_input("weight of the Laptop")
#Touchscreen
Touchscreen=st.selectbox("Touchscreen",["Yes","No"])
#IPS
Ips=st.selectbox("IPS",["Yes","No"])
#screen Size
Screen_size=st.number_input("Screen Size")
#resolution
Resolution=st.selectbox("Screen_Resolution",['1920x1080','1366x768','1600x900','3840x2160','3200x1800','2880x1800','2560x1600','2560x1440','2304x1440'])
#Cpu
Cpu=st.selectbox("Cpu_brand",options=df["cpu_brand"].unique())
#HDD
HDD=st.selectbox("HDD(In GB)",options=[0,128,256,512,1024,2048])
#SDD
SDD=st.selectbox("SDD(In GB)",options=[0,128,256,512,1024,2048])
#GPU
Gpu=st.selectbox("Gpu",options=df["Gpu_brand"].unique())
#OS
Os=st.selectbox("Os",options=df["Os"].unique())

if st.button("Predict Price"):
    #query
    X_res=int(Resolution.split("x")[0])
    Y_res=int(Resolution.split("x")[1])
    ppi=((X_res**2)+(Y_res**2))**0.5/Screen_size
    if Touchscreen=="Yes":
        Touchscreen=1
    else:
        Touchscreen=0
    if Ips=="Yes":
            Ips=1
    else:
        Ips=0
    
    query=np.array([company,type,Ram,weight,Touchscreen,Ips,ppi,Cpu,HDD,SDD,Gpu,Os])
    query=query.reshape(1,12)
    
    st.write("<p style='font-size: 20px;'>The predicted price of above configuration is:</p>", unsafe_allow_html=True)
    st.write("<p style='font-size: 20px;'>" + str(int(np.exp(pipe.predict(query))[0])) + "</p>", unsafe_allow_html=True)

    
    
    # st.title("The predicted price of above configuration is:\n",str(int(np.exp(pipe.predict(query))[0])))












