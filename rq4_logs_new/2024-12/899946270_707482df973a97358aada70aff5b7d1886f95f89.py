import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import zscore

# Load Data
@st.cache
def load_data(file):
    return pd.read_csv(file)

# Sidebar - File Upload
st.sidebar.header('Upload Solar Data')
uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    df = load_data(uploaded_file)
    st.write(f"Data Loaded Successfully! Dataset has {df.shape[0]} rows and {df.shape[1]} columns.")
    
    # Show first few rows of the dataset
    if st.sidebar.checkbox('Show Raw Data'):
        st.write(df.head())

    # Data Cleaning - Handle Missing Values and Negative Values
    if st.sidebar.checkbox('Handle Missing/Negative Values'):
        df['GHI'] = df['GHI'].apply(lambda x: x if x >= 0 else np.nan)
        df['DNI'] = df['DNI'].apply(lambda x: x if x >= 0 else np.nan)
        df['DHI'] = df['DHI'].apply(lambda x: x if x >= 0 else np.nan)
        df.fillna(df.mean(), inplace=True)
        st.write("Missing and Negative Values Handled.")

    # Sidebar - Select Features for Visualization
    st.sidebar.header("Select Features for Analysis")
    selected_columns = st.sidebar.multiselect('Select Columns to Plot:', df.columns.tolist(), default=['GHI', 'DNI', 'DHI', 'Tamb'])

    # Visualize Key Metrics
    st.header("Key Metrics")
    st.subheader("Time Series Plot")

    # Line Plot
    if 'Timestamp' in df.columns:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        st.line_chart(df.set_index('Timestamp')[selected_columns])

    # Correlation Matrix
    st.subheader("Correlation Matrix")
    corr = df[selected_columns].corr()
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(corr, annot=True, cmap="coolwarm", ax=ax)
    st.pyplot(fig)

    # Histograms
    st.subheader("Histogram for Selected Columns")
    for column in selected_columns:
        fig, ax = plt.subplots()
        df[column].plot(kind='hist', bins=30, ax=ax, title=f'Histogram of {column}')
        st.pyplot(fig)

    # Scatter Plot
    st.subheader("Scatter Plot")
    scatter_x = st.selectbox("Select X-axis for Scatter Plot", selected_columns)
    scatter_y = st.selectbox("Select Y-axis for Scatter Plot", selected_columns)
    fig, ax = plt.subplots()
    ax.scatter(df[scatter_x], df[scatter_y])
    ax.set_xlabel(scatter_x)
    ax.set_ylabel(scatter_y)
    st.pyplot(fig)

    # Z-Score Analysis
    st.subheader("Z-Score Analysis")
    z_scores = df[selected_columns].apply(zscore)
    st.write(z_scores.head())

    # Interactive Features
    st.sidebar.header("Interactive Analysis")
    feature_to_analyze = st.sidebar.selectbox("Select Feature for Detailed View", df.columns)
    st.write(df[[feature_to_analyze]].describe())

    # Filter Data Based on Specific Criteria
    st.sidebar.header("Filter Data")
    country_filter = st.sidebar.selectbox("Select Country", df['country'].unique())
    filtered_data = df[df['country'] == country_filter]
    st.write(f"Filtered Data for {country_filter}:", filtered_data)

# Footer
st.sidebar.markdown("### Created by Your Name")