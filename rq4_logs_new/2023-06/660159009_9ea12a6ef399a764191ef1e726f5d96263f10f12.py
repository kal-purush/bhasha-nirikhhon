import streamlit as st
import pickle
import pandas as pd
import numpy as np

model = pickle.load(open('pipe.pkl', 'rb'))

st.title("T20 Cricket Score Predictor")

teams = [
    'Australia',
    'India',
    'Bangladesh',
    'New Zealand',
    'South Africa',
    'England',
    'West Indies',
    'Afghanistan',
    'Pakistan',
    'Sri Lanka'
]

cities = ['Dubai',
          'Colombo',
          'Johannesburg',
          'Mirpur',
          'Auckland',
          'Dhaka',
          'Sydney',
          'Cape Town',
          'London',
          'Abu Dhabi',
          'Melbourne',
          'Pallekele',
          'Barbados',
          'Lahore',
          'Wellington',
          'Sharjah',
          'Centurion',
          'Christchurch',
          'Durban',
          'Lauderhill',
          'St Lucia',
          'Southampton',
          'Karachi',
          'Kolkata',
          'Hamilton',
          'Manchester',
          'Nottingham',
          'Mumbai',
          'Adelaide',
          'Cardiff',
          'Mount Maunganui',
          'Brisbane',
          'Chittagong',
          'Perth',
          'Delhi',
          'Nagpur',
          'Gros Islet',
          'Lucknow',
          'Chandigarh',
          'Ahmedabad',
          'Birmingham',
          'Bridgetown',
          "St George's",
          'Rajkot',
          'Bangalore',
          'St Kitts',
          'Trinidad'
          ]

batting_team_col, bowling_team_col = st.columns(2)

with batting_team_col:
    batting_team = st.selectbox('Select Batting Team', sorted(teams))
with bowling_team_col:
    bowling_team = st.selectbox('Select Bowling Team', sorted(teams))

city = st.selectbox('Select City', sorted(cities))

current_score_col, over_done_col, wickets_fall_col = st.columns(3)

with current_score_col:
    current_score = st.number_input('Current Score')
with over_done_col:
    overs = st.number_input('Overs Done (Works for over > 5)')
with wickets_fall_col:
    wickets = st.number_input('Wickets Fall')

last_five_over_runs = st.number_input('Runs scored in last 5 Overs')

if st.button('Predict Score'):
    balls_left = 120 - (overs*6)
    wickets_left = 10 - wickets
    crr = current_score / overs

    input_df = pd.DataFrame({
        'batting_team': [batting_team],
        'bowling_team': [bowling_team],
        'city': [city],
        'current_score': [current_score],
        'balls_left': [balls_left],
        'wickets_left': [wickets_left],
        'crr': [crr],
        'last_five': [last_five_over_runs]
    })

    result = model.predict(input_df)
    st.header("Predicted Score : " + str(int(result[0])))
