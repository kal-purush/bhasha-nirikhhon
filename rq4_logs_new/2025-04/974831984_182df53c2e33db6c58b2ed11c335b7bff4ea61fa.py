import streamlit as st
import hashlib
from cryptography.fernet import Fernet

KEY = Fernet.generate_key()
cipher = Fernet(KEY)

stored_data = {}
failed_attempts = 0

def hash_passkey(passkey):
    return hashlib.sha256(passkey.encode()).hexdigest()

def encrypt_data(text):
    return cipher.encrypt(text.encode()).decode()

def decrypt_data(encrypted_text):
    return cipher.decrypt(encrypted_text.encode()).decode()

st.set_page_config(page_title="Secure Vault", page_icon="🔒")
st.title("🔒 Secure Data Encryption System")

menu = ["Home", "Store Data", "Retrieve Data", "Login"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "Home":
    st.subheader("🏠 Welcome to the Secure Data System")
    st.write("Use this app to **securely store and retrieve data** using unique passkeys.")

elif choice == "Store Data":
    st.subheader("📂 Store Data Securely")
    username = st.text_input("Choose a Username:")
    text = st.text_area("Enter the Data You Want to Store:")
    passkey = st.text_input("Create a Passkey:", type="password")

    if st.button("Encrypt & Save"):
        if username and text and passkey:
            encrypted = encrypt_data(text)
            stored_data[username] = {
                "encrypted_text": encrypted,
                "passkey_hash": hash_passkey(passkey)
            }
            st.success(f"✅ Data stored for user '{username}'")
        else:
            st.error("⚠️ Please fill all fields.")

elif choice == "Retrieve Data":
    st.subheader("🔍 Retrieve Your Data")
    username = st.text_input("Enter Your Username:")
    passkey = st.text_input("Enter Your Passkey:", type="password")

    if st.button("Decrypt"):
        if username and passkey:
            user_entry = stored_data.get(username)

            if user_entry:
                hashed_input = hash_passkey(passkey)
                if hashed_input == user_entry["passkey_hash"]:
                    decrypted = decrypt_data(user_entry["encrypted_text"])
                    st.success(f"✅ Decrypted Data: {decrypted}")
                    failed_attempts = 0  # Reset
                else:
                    failed_attempts += 1
                    st.error(f"❌ Incorrect passkey! Attempts remaining: {3 - failed_attempts}")
            else:
                st.warning("⚠️ Username not found.")

            if failed_attempts >= 3:
                st.warning("🔒 Too many failed attempts! Redirecting to Login Page.")
                st.session_state['redirect_to_login'] = True
                st.experimental_rerun()
        else:
            st.error("⚠️ Both fields are required!")

elif choice == "Login":
    st.subheader("🔑 Reauthorization Required")
    master = st.text_input("Enter Master Password:", type="password")

    if st.button("Login"):
        if master == "admin123":
            failed_attempts = 0
            st.success("✅ Reauthorized successfully! Redirecting to Retrieve Data...")
            st.session_state['redirect_to_login'] = False
            st.experimental_rerun()
        else:
            st.error("❌ Incorrect Password!")