import streamlit as st
import requests

# FastAPI URL
API_BASE_URL = "http://127.0.0.1:8000"

st.title("Student Database Chat Agent")

# Get question from the user
user_question = st.text_input("Ask a question based on the student database:")

if st.button("Ask the Model"):
    if user_question:
        try:
            # Make request to FastAPI endpoint
            response = requests.get(f"{API_BASE_URL}/ask_agent/", params={"question": user_question})
            response_data = response.json()
            # Display model response
            st.success(response_data["response"])
        except Exception as e:
            st.error(f"Error: {str(e)}")
    else:
        st.warning("Please enter a question.")