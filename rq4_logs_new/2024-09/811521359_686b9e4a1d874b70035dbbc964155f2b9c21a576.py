import streamlit as st

st.set_page_config(page_title="My Multi-Page App", layout="wide")

st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select a page", ["Home", "App 1", "App 2"])

if page == "Home":
    st.title("Welcome to My Multi-Page App")
    st.write("Use the sidebar to navigate to different pages.")
elif page == "App 1":
    import pages.app1
    pages.app1.app()
elif page == "App 2":
    import pages.app2
    pages.app2.app()