import streamlit as st
import pandas as pd
import boto3 as bt
from botocore.config import Config
from botocore import UNSIGNED
import io

# Function to download file from S3
def download_from_s3(bucket_name, object_key):
    s3 = bt.client('s3', config=Config(signature_version=UNSIGNED))
    file = s3.get_object(Bucket=bucket_name, Key=object_key)
    return pd.read_parquet(io.BytesIO(file['Body'].read()))

# Load the data from S3
bucket_name = 'oedi-data-lake'
object_key = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/metadata/upgrade11.parquet'

# Download the parquet file and load into DataFrame
df = download_from_s3(bucket_name, object_key)

# Streamlit UI
st.title('House Filter Dashboard')

# Dropdown filters
climate_zone = st.selectbox(
    'Select Climate Zone',
    ['1A', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B', '6A', '6B']
)

state = st.selectbox(
    'Select State',
    ['FL', 'TX', 'LA', 'AZ', 'CA', 'TN', 'NY', 'IL', 'CO', 'MN', 'WY']
)

county = st.selectbox(
    'Select County',
    ['Miami-Dade County', 'Travis County', 'Jefferson Parish', 'Yuma County', 'Edwards County', 'Imperial County', 
     'Madison County', 'Los Angeles County', 'New York County', 'Yavapai County', 'Cook County', 'Erie County', 
     'Denver County', 'Ramsey County', 'Natrona County']
)

building_type = st.selectbox(
    'Select Building Type',
    ['Single-Family Detached', 'Single-Family Attached']
)



# Display selected filters
st.subheader('Selected Filters')
st.write(f'Climate Zone: {climate_zone}')
st.write(f'State: {state}')
st.write(f'County: {county}')
st.write(f'Building Type: {building_type}')
# Display filtered data
st.subheader('Filtered Data')
st.write(df)



# Apply filters based on user input
filtered_df = df[
    (df['in.ashrae_iecc_climate_zone_2004'] == climate_zone) &
    (df['in.state'] == state) &
    (df['in.county_name'] == county) &
    (df['in.geometry_building_type_acs'] == building_type)
]

# Show filtered data preview
st.subheader('Filtered Data Preview')
st.write(filtered_df)

# Provide download button for CSV
csv = filtered_df.to_csv(index=False)
st.download_button(
    label="Download Filtered Data as CSV",
    data=csv,
    file_name="filtered_house_data.csv",
    mime="text/csv"
)