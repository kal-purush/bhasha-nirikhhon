import streamlit as st
import sqlite3
import pandas as pd
import bcrypt

# Database connection
conn = sqlite3.connect('user_credentials.db')
c = conn.cursor()

# Ensure the users table exists and has the is_admin column
def setup_database():
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, password TEXT)''')
    
    # Check if is_admin column exists, if not, add it
    c.execute("PRAGMA table_info(users)")
    columns = [column[1] for column in c.fetchall()]
    if 'is_admin' not in columns:
        c.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0")
    
    conn.commit()

setup_database()

# Check if admin exists
def admin_exists():
    c.execute("SELECT COUNT(*) FROM users WHERE is_admin = 1")
    return c.fetchone()[0] > 0

# Admin authentication
def admin_login(username, password):
    c.execute("SELECT password, is_admin FROM users WHERE username = ?", (username,))
    result = c.fetchone()
    if result and result[1] == 1:  # Check if user is an admin
        return bcrypt.checkpw(password.encode('utf-8'), result[0])
    return False

# User management functions
def get_all_users():
    c.execute("SELECT username, is_admin FROM users")
    return c.fetchall()

def delete_user(username):
    c.execute("DELETE FROM users WHERE username = ?", (username,))
    conn.commit()

def toggle_admin(username):
    c.execute("UPDATE users SET is_admin = 1 - is_admin WHERE username = ?", (username,))
    conn.commit()

def add_user(username, password, is_admin=0):
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    try:
        c.execute("INSERT INTO users (username, password, is_admin) VALUES (?, ?, ?)", 
                  (username, hashed_password, is_admin))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

# First-time setup
def first_time_setup():
    st.title("First-Time Admin Setup")
    st.write("No admin user detected. Please create the first admin account.")
    
    admin_username = st.text_input("Admin Username")
    admin_password = st.text_input("Admin Password", type="password")
    confirm_password = st.text_input("Confirm Password", type="password")
    
    if st.button("Create Admin"):
        if admin_password == confirm_password:
            if add_user(admin_username, admin_password, is_admin=1):
                st.success("Admin account created successfully! Please log in.")
                st.rerun()
            else:
                st.error("An error occurred while creating the admin account.")
        else:
            st.error("Passwords do not match.")

# Main admin panel
def admin_panel():
    st.title("Admin Panel - GROW--MORE")

    # Display all users
    st.subheader("User Management")
    users = get_all_users()
    user_df = pd.DataFrame(users, columns=['Username', 'Is Admin'])
    user_df['Is Admin'] = user_df['Is Admin'].map({0: 'No', 1: 'Yes'})
    st.dataframe(user_df)

    # User actions
    st.subheader("User Actions")
    action = st.selectbox("Select Action", ["Delete User", "Toggle Admin Status"])
    username = st.selectbox("Select User", [user[0] for user in users if user[0] != st.session_state.get('admin_username')])

    if st.button("Perform Action"):
        if action == "Delete User":
            delete_user(username)
            st.success(f"User {username} has been deleted.")
        elif action == "Toggle Admin Status":
            toggle_admin(username)
            st.success(f"Admin status for {username} has been toggled.")
        st.rerun()

    # Add new user
    st.subheader("Add New User")
    new_username = st.text_input("New Username")
    new_password = st.text_input("New Password", type="password")
    is_admin = st.checkbox("Is Admin")
    if st.button("Add User"):
        if add_user(new_username, new_password, is_admin):
            st.success(f"New user {new_username} has been added.")
        else:
            st.error("Username already exists or an error occurred.")

# Main function
def main():
    st.set_page_config(page_title="Admin Panel - Stock Prediction App", layout="wide")

    if not admin_exists():
        first_time_setup()
    else:
        if 'admin_logged_in' not in st.session_state:
            st.session_state.admin_logged_in = False

        if not st.session_state.admin_logged_in:
            st.title("Admin Login")
            username = st.text_input("Admin Username")
            password = st.text_input("Password", type="password")
            if st.button("Login"):
                if admin_login(username, password):
                    st.session_state.admin_logged_in = True
                    st.session_state.admin_username = username
                    st.rerun()
                else:
                    st.error("Invalid credentials or not an admin user.")
        else:
            admin_panel()
            if st.sidebar.button("Logout"):
                st.session_state.admin_logged_in = False
                st.session_state.admin_username = None
                st.rerun()

if __name__ == "__main__":
    main()