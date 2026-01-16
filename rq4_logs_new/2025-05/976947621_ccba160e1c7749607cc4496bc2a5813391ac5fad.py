# app.py (Final Streamlit Cotton Survey)

import streamlit as st
import pandas as pd
import datetime
import os

# Ensure save folder exists
SAVE_DIR = 'cotton_survey_responses'
os.makedirs(SAVE_DIR, exist_ok=True)

# Translations placeholder (English only for now)
dict_translations = {
    'English': {
        'Language': 'Language', 'Farmer Profile': 'Farmer Profile', 'Farmer Name': 'Farmer Name',
        'Farmer Code': 'Farmer Code', 'Gender': 'Gender', 'Male': 'Male', 'Female': 'Female',
        'Farm Details': 'Farm Details', 'Farm Size (Acres)': 'Farm Size (Acres)',
        'Irrigation Source': 'Irrigation Source', 'Cotton Variety': 'Cotton Variety',
        'Sowing Date': 'Sowing Date', 'Seed Treatment': 'Seed Treatment', 'Yes': 'Yes', 'No': 'No',
        'Fertilizer Used': 'Fertilizer Used', 'Fertilizer Type': 'Fertilizer Type',
        'Quantity (Kg/Acre)': 'Quantity (Kg/Acre)', 'Pesticides Used': 'Pesticides Used',
        'Pesticide Type': 'Pesticide Type', 'Pesticide Quantity (ml/Acre)': 'Pesticide Quantity (ml/Acre)',
        'Yield Expected (Kg/Acre)': 'Yield Expected (Kg/Acre)', 'Name of Surveyor': 'Name of Surveyor',
        'Date of Visit': 'Date of Visit', 'Submit': 'Submit', 'Download CSV': 'Download CSV'
    }
}

# Streamlit Page Config
st.set_page_config(page_title="Cotton Survey", page_icon="🌿", layout="centered")

# Language Selection (only English currently)
lang = st.selectbox("Language", ("English",))
labels = dict_translations.get(lang, dict_translations['English'])

# Title
st.title(labels['Farmer Profile'])

# Form Start
with st.form("cotton_survey_form"):
    st.header(labels['Farmer Profile'])
    farmer_name = st.text_input(labels['Farmer Name'])
    farmer_code = st.text_input(labels['Farmer Code'])
    gender = st.selectbox(labels['Gender'], (labels['Male'], labels['Female']))

    st.header(labels['Farm Details'])
    farm_size = st.number_input(labels['Farm Size (Acres)'], min_value=0.0)
    irrigation = st.text_input(labels['Irrigation Source'])
    variety = st.text_input(labels['Cotton Variety'])
    sowing_date = st.date_input(labels['Sowing Date'])
    seed_treatment = st.selectbox(labels['Seed Treatment'], (labels['Yes'], labels['No']))

    st.header(labels['Fertilizer Used'])
    fert_type = st.text_input(labels['Fertilizer Type'])
    fert_qty = st.number_input(labels['Quantity (Kg/Acre)'], min_value=0.0)

    st.header(labels['Pesticides Used'])
    pest_type = st.text_input(labels['Pesticide Type'])
    pest_qty = st.number_input(labels['Pesticide Quantity (ml/Acre)'], min_value=0.0)

    yield_expected = st.number_input(labels['Yield Expected (Kg/Acre)'], min_value=0.0)
    surveyor = st.text_input(labels['Name of Surveyor'])
    visit_date = st.date_input(labels['Date of Visit'])

    submit = st.form_submit_button(labels['Submit'])

if submit:
    now = datetime.datetime.now()
    data = {
        'Timestamp': now.isoformat(),
        'Language': lang,
        'Farmer Name': farmer_name,
        'Farmer Code': farmer_code,
        'Gender': gender,
        'Farm Size (Acres)': farm_size,
        'Irrigation Source': irrigation,
        'Cotton Variety': variety,
        'Sowing Date': sowing_date.isoformat(),
        'Seed Treatment': seed_treatment,
        'Fertilizer Type': fert_type,
        'Fertilizer Quantity (Kg/Acre)': fert_qty,
        'Pesticide Type': pest_type,
        'Pesticide Quantity (ml/Acre)': pest_qty,
        'Yield Expected (Kg/Acre)': yield_expected,
        'Surveyor Name': surveyor,
        'Date of Visit': visit_date.isoformat()
    }

    df = pd.DataFrame([data])
    filename = f"cotton_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(os.path.join(SAVE_DIR, filename), index=False)
    st.success("Submitted successfully!")
    st.download_button(label=labels['Download CSV'], data=df.to_csv(index=False).encode(), file_name=filename, mime='text/csv')