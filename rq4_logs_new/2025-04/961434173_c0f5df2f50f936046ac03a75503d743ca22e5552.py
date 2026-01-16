import streamlit as st
from datetime import date
import json
# Configure page
st.set_page_config(
    page_title="Modern Dashboard",
    page_icon="📊", 
    layout="wide"
)

# Sidebar navigation
with st.sidebar:
    st.title("Navigation")
    page = st.radio("Go to", ["Dashboard", "Analytics", "Settings"])
    
    st.divider()
    st.write(f"Today: {date.today().strftime('%B %d, %Y')}")

# Main content area
st.title("📊 Modern Dashboard")
st.subheader("Upload CSV File")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    try:
        import pandas as pd
        # Read the CSV file
        df = pd.read_csv(uploaded_file)
        
        # Display the content of the CSV
        st.write("### Uploaded CSV Content")
        st.dataframe(df)

        # Initialize session state
        if 'original_df' not in st.session_state:
            st.session_state.original_df = df.copy()
            st.session_state.edited_df = df.copy()
            st.session_state.edited_rows = set()

        # Track edits
        def on_edit():
            edited_rows = set()
            for idx in range(len(st.session_state.edited_df)):
                if not st.session_state.original_df.iloc[idx].equals(st.session_state.edited_df.iloc[idx]):
                    edited_rows.add(idx)
            st.session_state.edited_rows = edited_rows

        # Add Train column to DataFrame
        st.session_state.edited_df['Train'] = [False] * len(st.session_state.edited_df)
        
        # Create columns for table and buttons
        col1, col2 = st.columns([0.9, 0.1])

        with col1:
            # Display editable DataFrame
            edited_df = st.data_editor(
                st.session_state.edited_df,
                disabled=["Train"],
                hide_index=True,
                on_change=on_edit
            )

        with col2:
            # Add Train buttons for each row
            st.write("")  # Empty space for alignment
            for idx in range(len(edited_df)):
                if idx in st.session_state.edited_rows:
                    if st.button(f"Train", key=f"train_{idx}"):
                        row = edited_df.iloc[idx].drop('Train')
                        json_data = json.dumps(row.to_dict(), indent=2)
                        print(f"Training data for row {idx+1}:\n{json_data}")
                        st.toast(f"Sent row {idx+1} to training pipeline")

        # Update session state (remove Train column if it exists)
        if 'Train' in edited_df.columns:
            st.session_state.edited_df = edited_df.drop(columns=['Train'])
        else:
            st.session_state.edited_df = edited_df

        # Download button
        if st.button("Download Updated CSV"):
            st.session_state.edited_df.to_csv("updated_file.csv", index=False)
            st.success("Updated file is ready for download!")
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
st.caption("A responsive Streamlit application")

# Create responsive columns
col1, col2 = st.columns(2)

with col1:
    st.metric(label="Total Users", value="1,234", delta="12%")
    st.write("""
    ### Recent Activity
    - User signups: 45
    - Page views: 1,234
    - Conversions: 32%
    """)

with col2:
    st.metric(label="Revenue", value="$12,345", delta="-3%")
    st.write("""
    ### Performance
    - Load time: 1.2s
    - Uptime: 99.9%
    - Active sessions: 56
    """)

# Responsive chart section
st.subheader("Analytics Overview")
st.line_chart({
    'Users': [100, 200, 300, 400, 500, 600],
    'Revenue': [50, 60, 70, 80, 90, 100]
})

# Footer
st.divider()
st.caption("© 2023 Modern Dashboard - All rights reserved")