import numpy as np 
import pandas as pd 
import joblib
import streamlit as st

st.set_page_config(
    page_title="Football Prediction HSV", 
    layout="wide",                  
    initial_sidebar_state="collapsed")

scaler=joblib.load('football_scaler.joblib')

@st.cache_resource

def load_model(file_path):
    return joblib.load(file_path)

football_model=joblib.load('log_model.joblib')

st.title("Football Probability Prediction")

hthg=st.number_input("Enter home team home goals : ",min_value=0,value=0)
htag=st.number_input("Enter home team away goals : ",min_value=0,value=0)
Team_A_Shots=st.number_input("Enter team A shots : ",min_value=0,value=0)
Team_B_Shots=st.number_input("Enter team B shots : ",min_value=0,value=0)
Team_A_ShotsOnTarget=st.number_input("Enter team A shots on target : ",min_value=0,value=0)
Team_B_ShotsOnTarget=st.number_input("Enter team B shots on target : ",min_value=0,value=0)
Team_A_Corners=st.number_input("Enter team A corners : ",min_value=0,value=0)
Team_B_Corners=st.number_input("Enter team B corners : ",min_value=0,value=0)
Team_A_Fouls=st.number_input("Enter team A fouls : ",min_value=0,value=0)
Team_B_Fouls=st.number_input("Enter team B fouls: ",min_value=0,value=0)
Team_A_Possession=st.number_input("Enter team A possession % : ",min_value=0.00,value=0.00,format="%.3f")
Team_B_Possession=st.number_input("Enter team B possession % : ",min_value=0.00,value=0.00,format="%.3f")
Home_Advantage=st.selectbox("Team has home advantage : ",['True','False'])
Team_A_Goals_Last5=st.number_input("Enter team A goals last 5 matches : ",min_value=0.00,value=0.00,format="%.3f")
Team_B_Goals_Last5=st.number_input("Enter team B goals last 5 matches : ",min_value=0.00,value=0.00,format="%.3f")
Team_A_Shots_Last5=st.number_input("Enter team A shots last 5 matches : ",min_value=0.00,value=0.00,format="%.3f")
Team_B_Shots_Last5=st.number_input("Enter team B shots last 5 matches : ",min_value=0.00,value=0.00,format="%.3f")
Team_A_ShotsOnTarget_Last5=st.number_input("Enter team A shots on target last 5 matches : ",min_value=0.00,value=0.00,format="%.3f")
Team_B_ShotsOnTarget_Last5=st.number_input("Enter team B shots on target last 5 matches : ",min_value=0.00,value=0.00,format="%.3f")

Home_Advantage = 1 if Home_Advantage == "True" else 0

if st.button("Predict"):
    input_data=np.array([[hthg,htag,Team_A_Shots,Team_B_Shots,Team_A_ShotsOnTarget,Team_B_ShotsOnTarget,Team_A_Corners,Team_B_Corners,Team_A_Fouls,Team_B_Fouls,Team_A_Possession,Team_B_Possession,Home_Advantage,Team_A_Goals_Last5,Team_B_Goals_Last5,Team_A_Shots_Last5,Team_B_Shots_Last5,Team_A_ShotsOnTarget_Last5,Team_B_ShotsOnTarget_Last5]])
    input_data=input_data.reshape(1,-1)
    input_data=scaler.transform(input_data)
    prediction = football_model.predict(input_data)
    result_map = {0: 'Away Win', 1: 'Draw', 2: 'Home Win'}
    st.write(result_map[prediction[0]])