import streamlit as st 
import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import pandas_datareader as data 
import yfinance as yf
from datetime import date
from statsmodels.tsa.arima.model import ARIMA
import statsmodels.api as sm



st.markdown('## Stock :orange[Web] :red[App]')
st.image("https://th.bing.com/th/id/OIP.Ttoy04OYis0BLYxlPZvTSwHaEK?w=331&h=186&c=7&r=0&o=5&dpr=1.3&pid=1.7")
st.markdown('## Stock :red[Future] :green[Prediction]')

stocks=st.text_input('Enter Stock Ticker','SBIN.NS')
Start='2015-01-01'
Today = date.today().strftime("%Y-%m-%d")

data=yf.Ticker(stocks)
hys_data=data.history(period='10y')
hys_data.reset_index(inplace=True)



data,prediction1,prediction2=st.tabs(['Data','Prediction1','Prediction2'])

with data:
    st.subheader(":orange[Raw] :green[Data]")
    st.write(hys_data.tail(10))

with prediction1:
     high,low,close=st.tabs(['High','Low','Close'])
     
     with high:
         df1=hys_data.reset_index()['High']
         model=ARIMA(df1,order=(2,1,3))
         result=model.fit()
         forecast_steps=5
         forecast_values=result.predict(start=len(df1),end=len(df1)+forecast_steps-1,dynamic=False)
         st.write(forecast_values)
         
     with low:
         df1=hys_data.reset_index()['Low']
         model=ARIMA(df1,order=(2,1,3))
         result=model.fit()
         forecast_steps=5
         forecast_values=result.predict(start=len(df1),end=len(df1)+forecast_steps-1,dynamic=False)
         st.write(forecast_values) 
         
     with close:
         df1=hys_data.reset_index()['Close']
         model=ARIMA(df1,order=(2,1,3))
         result=model.fit()
         forecast_steps=5
         forecast_values=result.predict(start=len(df1),end=len(df1)+forecast_steps-1,dynamic=False)
         st.write(forecast_values)
         
order=(1,1,1)
seasonal_order=(1,1,1,12)   
model2=sm.tsa.SARIMAX(df1,order=order,seasonal_order=seasonal_order)   
result=model2.fit() 
forecast_steps=5   
forecast_values=result.predict(start=len(df1),end=len(df1)+forecast_steps-1,dynamic=False)   
with prediction2:
     high,low,close=st.tabs(['High','Low','Close'])
     
     with high:
        df1=hys_data.reset_index()['High']
        model2=sm.tsa.SARIMAX(df1,order=order,seasonal_order=seasonal_order)   
        result=model2.fit() 
        forecast_steps=5    
        forecast_values=result.predict(start=len(df1),end=len(df1)+forecast_steps-1,dynamic=False)          
        st.write(forecast_values)
     with low:
        df1=hys_data.reset_index()['Low']
        model2=sm.tsa.SARIMAX(df1,order=order,seasonal_order=seasonal_order)   
        result=model2.fit() 
        forecast_steps=5    
        forecast_values=result.predict(start=len(df1),end=len(df1)+forecast_steps-1,dynamic=False)          
        st.write(forecast_values) 
            
     with close:
        df1=hys_data.reset_index()['Close']
        model2=sm.tsa.SARIMAX(df1,order=order,seasonal_order=seasonal_order)   
        result=model2.fit() 
        forecast_steps=5    
        forecast_values=result.predict(start=len(df1),end=len(df1)+forecast_steps-1,dynamic=False)          
        st.write(forecast_values)