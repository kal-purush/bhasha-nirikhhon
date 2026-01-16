import streamlit as st

def client_inputs():
    st.subheader("📢 Generate a Cold Email for Client Outreach")
    your_name = st.text_input("👤 Your Name")
    company = st.text_input("🏢 Your Company Name")
    client_name = st.text_input("👤 Client's Name")
    client_company = st.text_input("🏢 Client's Company Name")
    service_offered = st.text_area("🛠 Service/Product You're Offering")
    unique_value = st.text_area("✨ Unique Value Proposition")


    all_fields_filled = all([
        your_name, company, client_name, client_company, service_offered, unique_value
    ])

    if not all_fields_filled:
        st.warning("⚠️ Please fill out all required fields before generating the cold email.")

    return {
        "your_name": your_name,
        "company": company,
        "client_name": client_name,
        "client_company": client_company,
        "service_offered": service_offered,
        "unique_value": unique_value,
        "all_fields_filled": all_fields_filled  # Flag to indicate completion
    }