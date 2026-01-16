import streamlit as st
import pandas as pd
import yfinance as yf
import datetime
import matplotlib.pyplot as plt

# 1️⃣ Using st.write() (Auto-Formats Tables)

data = {
    "Feature": ["Purpose", "Supports", "Where it Appears?", "Use Case"],
    "st.write()": [
        "Displays content on the Streamlit web app UI",
        "Text, numbers, dataframes, plots, markdown, HTML, etc.",
        "Inside the browser/web app",
        "For user-friendly display of content"
    ],
    "print()": [
        "Prints output to the terminal/console",
        "Text-only (used for debugging)",
        "Inside the terminal (not visible in the app)",
        "Debugging/testing in the backend"
    ]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Display Table
st.write("### Comparison Table: `st.write()` vs `print()`")
st.write(df)

# Using st.dataframe() (Interactive Table with Scrolling & Sorting)
st.dataframe(df)

# Using st.table() (Static Styled Table)
st.table(df)

# Using st.data_editor() (Editable Table)
st.data_editor(df)