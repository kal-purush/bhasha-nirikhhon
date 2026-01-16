import streamlit as st
import hashlib
import sqlite3
from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import RobustScaler
import matplotlib.pyplot as plt
from st_aggrid import AgGrid, GridOptionsBuilder
from streamlit_autorefresh import st_autorefresh

# Database connection
def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn

# Hashing the URL
def hash_url(url):
    return hashlib.sha256(url.encode()).hexdigest()

# Handle user login
def login_user(username, password):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password)).fetchone()
    conn.close()
    return user

# Handle user registration
def register_user(username, password, subscription):
    conn = get_db_connection()
    conn.execute('INSERT INTO users (username, password, subscription, created_at) VALUES (?, ?, ?, ?)',
                 (username, password, subscription, datetime.now()))
    conn.commit()
    conn.close()

# Logout functionality
def logout():
    st.session_state.user = None
    st.session_state.subscription = None
    st.session_state.show_register_form = False
    st.success("Logged out successfully")

# Login view
def login_view():
    st.subheader("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login", key="login_button"):
        user = login_user(username, password)
        if user:
            st.session_state.user = username
            st.session_state.subscription = user["subscription"]
            st.success("Logged in successfully!")
        else:
            st.error("Invalid username or password")

    st.write("Don't have an account?")
    if st.button("Register", key="show_register_button"):
        st.session_state.show_register_form = True

# Registration view
def register_view():
    st.subheader("Register")
    username = st.text_input("Username (for registration)")
    password = st.text_input("Password (for registration)", type="password")
    subscription = st.selectbox("Subscription Type", ['free', 'pro', 'premium', 'on demand'])

    if st.button("Register", key="register_button"):
        register_user(username, password, subscription)
        st.success("Registration successful! Please login.")
        st.session_state.show_register_form = False
    
    if st.button("Back to Login", key="back_to_login_button"):
        st.session_state.show_register_form = False

# Dashboard functionality
def run_dashboard():
    import MLD_main

    st.title('ML Dashboard')

    selected_option = st.sidebar.selectbox(
        "Choose Dataset",
        ("None", "all_data_bike2", "house_price_data", "cyclonePreheater","inverter_data",
        "KAG_conversion_data2", "Sunspot", "out_clean", "winequality-white2", "other", "url", "g_sheet")
    )
    # selected_option = st.sidebar.selectbox(
    #    "Choose Dataset",
    #    ("None","regression","classification", "time series")
    # )

    with st.sidebar:
        st_m_type = st.selectbox(
            "select type", ("None", "regression", "classification", "time series", "pipe"))
        #st.write('* type: ',0,1,2,'regression')
        #st.write('* type: ',0,'= not a time data with normal ML')
        #st.write(1,',',2,'= time data with normal ML')
        #st.write(3,'= time_series')
        st.write(
            '** Use pipe only for kc_house_data.csv (available on kaggle), due to its testing phase')
        if st_m_type == 'None':
            st_type = 0
        elif st_m_type == 'regression':
            st_type = 1
        elif st_m_type == 'classification':
            st_type = 2
        elif st_m_type == 'time series':
            st_type = 3
        elif st_m_type == 'pipe':
            st_type = 4
        else:
            st.write('selct a model type')

        display = st.button('display')

        if apply_cicd := st.button('Apply CI'):
            st.write(":smile:")

        st.write('choose how you want to enter a data as a google sheet or just upload')
        data_file_up = st.selectbox(
            "how to upload a data", ("Drag or browse file", "google sheet"))

        if data_file_up == "Drag or browse file":
            # with st.sidebar:
            data_file = st.file_uploader("Upload CSV", type=['csv'])
            st.write(data_file)
            if data_file:
                df = pd.read_csv(data_file)
                target = st.selectbox("select target", df.columns)
                #data(df, target, type)
                #import time_series as ts
                # @st.cache(ttl=7200)
                #ts.data(df, target)

        elif data_file_up == "google sheet":
            #public_gsheets_url = "https://docs.google.com/spreadsheets/d/1lh9YUjvYfRIO7o88wgA3fpwxTlWrMKe36S5TFRwzbdI/edit?usp=sharing"

            # Create a connection object.
            conn = connect()
            with st.sidebar:
                public_gsheets_url = st.text_input('The google sheet URL link')
                st.write('only press enter with keyboard')
                st.write('DO NOT HIT PROCESS BEFORE SELECTING TARGET')
            #connected = http.client.HTTPConnection(public_gsheets_url)
            if public_gsheets_url:

                # Perform SQL query on the Google Sheet.
                # Uses st.cache to only rerun when the query changes or after 10 min = 600 ttl.
                # @st.experimental_memo(ttl=300)
                # @st.cache(ttl=75)
                st.write(public_gsheets_url)

                def run_query(query):
                    rows = conn.execute(query, headers=1)  # , headers=1
                    rows = rows.fetchall()  # st.write('runned sucess')
                    return rows

                #sheet_url = st.secrets["public_gsheets_url"]
                sheet_url = public_gsheets_url
                rows = run_query(f'SELECT * FROM "{sheet_url}"')
                st.write('runned sucess')
                #st.write('rows are:',rows)
                gs_df = pd.DataFrame(rows)

                # Print results.
                # for row in rows:
                #st.write(f"{row.Name} has {row.age} age")
                if gs_df.shape[0] > 30:
                    # with st.spinner('Loading your data from google sheets... \n please wait for a bit'):
                    df = gs_df
                    data_file = 'google sheet'
                    target = st.selectbox("select target", df.columns)

                else:
                    st.write(gs_df)
                    #target = st.selectbox("select target",gs_df.columns)
                    # st.write(target)
            else:
                st.write('enter the url, then again click display button')

            # with st.sidebar:
        # st.write(type(df))
        #data_dict = df.to_dict()
        # display
        # st.write(type(data_dict))
        #df = pd.DataFrame.from_dict(data_dict)
        # st.write(type(df))
        process = st.button("Process")

    if process and data_file is not None:
        # st.dataframe(df)
        data(df, target, st_type)
        # data(data_file,'',st_type)
        # process=False
    st.write(process)
    # except Exception:
    #st.write('either select file or upload a file....')
    if display:
        if selected_option == 'None':
            st.write('NO DataSet selected')
            st.write(st_type)
            col1, col2, col3 = st.columns(3)
            #a,b,c = st.columns(3)
            st.write(type(col1))
            with col1:
                st.header("A cat")
                st.image("https://static.streamlit.io/examples/cat.jpg")

            with col2:
                st.header("A dog")
                st.image("https://static.streamlit.io/examples/dog.jpg")

            with col3:
                st.header("An owl")
                st.image("https://static.streamlit.io/examples/owl.jpg")

        elif selected_option == 'all_data_bike2':
            """ data with datetime """
            st.write('type 1 and type 3 compatible only')
            if st_type in [1]:
                data('all_data_bike2.csv', 'count', st_type)
            else:
                st.write('select appropriate type')

        elif selected_option == 'upload':
            data_file = st.file_uploader("Upload CSV", type=['csv'])
            file_details = {"Filename": data_file.name, "FileType": data_file.type,
                            "FileSize (in kb)": round(data_file.size, 3)/1024}
            st.write(file_details)
            df = pd.read_csv(data_file)
            target = st.selectbox("select target", df.columns)
            # if selected_option in df.columns:
            process = st.button("Process")

            if process and data_file is not None:
                st.dataframe(df)
                data(df, target, st_type)

        elif selected_option == 'house_price_data':
            st.write('type 1,2 and type 3 compatible only')
            """ data with datetime """
            if st_type in [1, 3]:
                # data('cyclonePreheater.csv','Cyclone_Gas_Outlet_Temp',2)
                data('data.csv', 'price', st_type)
            else:
                st.write('select appropriate type')

        elif selected_option == 'inverter_data':
            st.write('type 1,2 and type 3 compatible only')
            """ data with datetime """
            if st_type in [1, 3]:
                # data('cyclonePreheater.csv','Cyclone_Gas_Outlet_Temp',2)
                data('inverter_data3.csv', '1104500527', st_type)
            else:
                st.write('select appropriate type')
        elif selected_option == 'cyclonePreheater':
            st.write('type 1,2 and type 3 compatible only')
            """ data with datetime """
            if st_type in [1, 3]:
                # data('cyclonePreheater.csv','Cyclone_Gas_Outlet_Temp',2)
                data('cyclonePreheater7.csv', 'Cyclone_Gas_Outlet_Temp', st_type)
            else:
                st.write('select appropriate type')
        elif selected_option == 'KAG_conversion_data2':
            """ data without datetime """
            st.write('type 0 compatible only')
            if st_type in [1, 2]:
                data('KAG_conversion_data2.csv', 'conv_rate', st_type)
            else:
                st.write('select appropriate type')
        elif selected_option == 'Sunspot':
            """ data with datetime """
            st.write('type 1 and type 3 compatible only')
            if st_type in [1, 3]:
                data('Sunspots.csv', 'Monthly Mean Total Sunspot Number', st_type)
            else:
                st.write('select appropriate type')
        elif selected_option == 'out_clean':
            """ data with datetime """
            st.write('type 1 and type 3 compatible only')
            if st_type in [1, 3]:
                data('Out_clean.csv',
                    'Total_Generation_kwh', st_type)
            else:
                st.write('select appropriate type')
        elif selected_option == 'winequality-white2':
            """ data without datetime """
            st.write('type 0 compatible only')
            if st_type in [1, 2]:
                data('winequality-white2.csv', 'quality', st_type)
            else:
                st.write('select appropriate type')
        elif selected_option == 'other':
            """ other data's """
            data('climate.xml')
        elif selected_option == 'url':
            """data from url"""
            url = 'https://raw.githubusercontent.com/JoaquinAmatRodrigo/skforecast/master/data/h2o_exog.csv'
            data(url, 'y', 4)
        elif selected_option == 'g_sheet':
            st.write('option entered for g-sheet')

            #public_gsheets_url = "https://docs.google.com/spreadsheets/d/1lh9YUjvYfRIO7o88wgA3fpwxTlWrMKe36S5TFRwzbdI/edit?usp=sharing"

            # Create a connection object.
            conn = connect()
            with st.sidebar:
                public_gsheets_url = st.text_input('The URL link')
            #connected = http.client.HTTPConnection(public_gsheets_url)
            if public_gsheets_url != '':
                # Perform SQL query on the Google Sheet.
                # Uses st.cache to only rerun when the query changes or after 10 min = 600 ttl.
                # @st.experimental_memo(ttl=300)
                # @st.cache(ttl=75)
                def run_query(query):
                    rows = conn.execute(query, headers=1)
                    rows = rows.fetchall()
                    #st.write('runned sucess')
                    return rows

                #sheet_url = st.secrets["public_gsheets_url"]
                sheet_url = public_gsheets_url
                rows = run_query(f'SELECT * FROM "{sheet_url}"')
                st.write('runned sucess')
                #st.write('rows are:',rows)
                gs_df = pd.DataFrame(rows)
                st.write(gs_df)

                # Print results.
                # for row in rows:
                #st.write(f"{row.Name} has {row.age} age")
            else:
                st.write('enter the url, then again click display button')

        # st.experimental_memo.clear()
        st.write('done')
        # st.rerun(120)
        # st.experimental_rerun()
        st.stop()



    # You can add more functions and components from MLD.py as needed

def main():
    st.set_page_config(page_title="ML-dashboard", page_icon="🧊", layout="wide")
    st.title("ML Dashboard with Authentication")

    if "user" not in st.session_state:
        st.session_state.user = None
        st.session_state.subscription = None
    if "show_register_form" not in st.session_state:
        st.session_state.show_register_form = False

    if st.session_state.user:
        st.sidebar.write(f"Logged in as {st.session_state.user}")
        st.sidebar.button("Logout", key="logout_button", on_click=logout)
        
        if st.session_state.subscription in ['pro', 'premium']:
            run_dashboard()
        else:
            st.write("You're on a free or on-demand subscription. Upgrade to access the full dashboard.")
            
            # Add your dashboard code here
            # For example:
            data = pd.DataFrame(np.random.randn(20, 3), columns=['a', 'b', 'c'])
            st.line_chart(data)
            
            # Add more components from your original MLD.py here
            # For example:
            if st.button("Run Analysis"):
                # Placeholder for analysis function
                st.write("Analysis complete!")
                        

    else:
        if st.session_state.show_register_form:
            register_view()
        else:
            login_view()

if __name__ == '__main__':
    main()


