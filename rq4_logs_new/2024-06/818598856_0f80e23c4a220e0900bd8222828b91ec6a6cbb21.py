import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import hashlib

# Function to hash passwords
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Function to verify hashed passwords
def verify_password(stored_password, provided_password):
    return stored_password == hash_password(provided_password)

# Function to create SQLite database table if not exists and truncate the table
def create_table():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS sales')
    c.execute('''
        CREATE TABLE sales (
            sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT,
            customer_id TEXT,
            date TEXT,
            cost REAL
        )
    ''')
    conn.commit()
    conn.close()

# Function to insert data from CSV into SQLite database
def insert_data(file):
    conn = sqlite3.connect('database.db')
    df = pd.read_csv(file)
    df.to_sql('sales', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()

# Function to fetch data from SQLite database
def fetch_data():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('SELECT sales_id, customer_id, country, cost FROM sales')
    data = c.fetchall()
    conn.close()
    return data

# Function to authenticate user
def authenticate_user(username, password):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('SELECT password FROM admin WHERE username = ?', (username,))
    result = c.fetchone()
    conn.close()
    if result:
        stored_password = result[0]
        return verify_password(stored_password, password)
    return False

# Function to setup initial admin credentials (run once)
def setup_admin():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS admin (username TEXT PRIMARY KEY, password TEXT)')
    c.execute('SELECT * FROM admin WHERE username = ?', ('admin',))
    if not c.fetchone():
        c.execute('INSERT INTO admin (username, password) VALUES (?, ?)', ('admin', hash_password('admin')))
    conn.commit()
    conn.close()

# Function to generate the UI
def generate_ui():
    st.set_page_config(
        page_title="Sales Dashboard",
        page_icon="📊",
        layout="wide"
    )

    setup_admin()

    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            if authenticate_user(username, password):
                st.session_state.authenticated = True
                st.success("Login successful")
                st.experimental_rerun()  # Rerun the script to update the state
            else:
                st.error("Invalid username or password")
        return

    create_table()

    st.title("Sales Dashboard")
    st.markdown(
        """
        Welcome aspiring innovators! In this code fest, you'll embark on a journey of creativity and problem-solving.
        Get ready to collaborate, code, and make a lasting impact!

        ### Problem statement:
        Create a sales dashboard that tracks the sales of a given company over the period of a few months.
        The dashboard should have the following functionalities:

        1. Basic auth functionality.
        2. CSV File upload and populate data in the DropDown and also store that CSV in the Database.
        3. Multi Select Searchable Dropdown.
        4. Image Upload and store it in the Database.
        5. Data visualization such as bar charts or line charts.

        Duration: 3 hours
        """
    )

    # File upload and database insertion
    uploaded_file = st.file_uploader("Upload a CSV file", type="csv")
    if uploaded_file is not None:
        insert_data(uploaded_file)
        st.success("File uploaded successfully and data stored in the database.")

    # Dropdown to select data from database
    data = fetch_data()
    if data:
        df = pd.DataFrame(data, columns=['Sales ID', 'Customer ID', 'Country', 'Cost'])
        
        sales_ids = df['Sales ID'].unique().tolist()
        customer_ids = df['Customer ID'].unique().tolist()
        countries = df['Country'].unique().tolist()

        st.subheader("Select Sales ID")
        selected_sales_id = st.multiselect("Choose Sales ID", sales_ids, default=sales_ids)

        st.subheader("Select Customer ID")
        selected_customer_id = st.multiselect("Choose Customer ID", customer_ids, default=customer_ids)

        st.subheader("Select Country")
        selected_country = st.multiselect("Choose Country", countries, default=countries)

        # Filter data based on selected dropdown values
        filtered_df = df[
            (df['Sales ID'].isin(selected_sales_id)) &
            (df['Customer ID'].isin(selected_customer_id)) &
            (df['Country'].isin(selected_country))
        ]

        # Visualization
        st.subheader("Visualizations")

        # Bar chart for country distribution
        st.write("### Country Distribution")
        country_counts = filtered_df['Country'].value_counts()
        fig, ax = plt.subplots()
        sns.barplot(x=country_counts.index, y=country_counts.values, ax=ax)
        ax.set_xlabel('Country')
        ax.set_ylabel('Count')
        st.pyplot(fig)

        # Pie chart for cost ranges
        st.write("### Cost Ranges")
        cost_ranges = pd.cut(filtered_df['Cost'], bins=[0, 50, 100, 150, 200, float('inf')], labels=['0-50', '51-100', '101-150', '151-200', '200+'])
        cost_range_counts = cost_ranges.value_counts()
        fig, ax = plt.subplots()
        ax.pie(cost_range_counts, labels=cost_range_counts.index, autopct='%1.1f%%', startangle=90)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        st.pyplot(fig)

# Main function
if __name__ == "__main__":
    generate_ui()