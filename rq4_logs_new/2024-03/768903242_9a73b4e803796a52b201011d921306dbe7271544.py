import streamlit as st
import pandas as pd  
import geopandas as gpd
from joblib import load
import numpy as np
import folium
from streamlit_folium import folium_static




gps_data =  pd.read_csv('Data/islington_info.csv')
postcodes_islington = gps_data['postcode'].str.split(pat = ' ', expand = True)
postcodes_islington.columns =['first_part', 'second_part']
df2 = pd.DataFrame(postcodes_islington, columns=['first_part', 'second_part'])
postcode_dict = df2.groupby('first_part')['second_part'].apply(list).to_dict()
geodf =  pd.read_csv('Data/merged_data.csv')
avg_price = geodf[geodf.year == 2023].groupby('wards')['price'].mean().reset_index(name='avg_price')
gdf = gpd.GeoDataFrame(geodf, geometry=gpd.points_from_xy(geodf.longitude, geodf.latitude),
        crs="EPSG:4326" ) 
property_type_dic = {"Flat/Maisonnette": 1, "Terrace House": 3, "Semi-detached House": 2, "Detached House": 0}
duration_dic = {"Freehold": 0, "Leasehold": 1}
month_dic = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}
pipeline = load('XGBoost2.joblib')

def main():
    st.title("Islington Price Estimator 2024")
    


    postcode1 = st.selectbox("Postcode District (Islington)", postcode_dict.keys())
    postcode2 = st.selectbox("Second Part of Postcode", postcode_dict[postcode1])
    number_rooms = st.selectbox("Number of Rooms", range(1, 11))
    total_floor_area = st.number_input("Total Floor Area (sq metres)", value=50)
    month_of_transfer = st.selectbox("Month of Transfer", {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12})
    duration = st.selectbox("Duration", {"Freehold": "Freehold", "Leasehold": "Leasehold"})
    property_type = st.selectbox("Property Type", {"Flat/Maisonnette": "F", "Terrace House": "T", "Semi-detached House": "S", "Detached House": "D"})
    current_energy_efficiency = st.slider("Current Energy Efficiency", min_value=0, max_value=100, value=50)
    potential_energy_efficiency = st.slider("Potential Energy Efficiency", min_value=0, max_value=100, value=50)
    construction_age_band = st.selectbox("Construction Age Band", gdf.CONSTRUCTION_AGE_BAND.unique())

    # Button to submit or process the data
    if st.button("Submit"):
        # Collect user inputs 
        postcode = postcode1 + " " + postcode2  # Combine postcode parts
       
        # Find GPS info (you'll need to implement the matching logic)
        gps_info = find_gps_from_postcode(postcode1, postcode2, gps_data) 
        average_year = get_average_year(construction_age_band )
        average_price = get_avg_price(avg_price, gps_info['wards'].values[0])
        X_new = [gps_info.latitude.values[0], gps_info.longitude.values[0], gps_info.closest_station_1_distance.values[0],
        gps_info.closest_station_2_distance.values[0], gps_info.closest_station_3_distance.values[0], gps_info.Academy.values[0],
        gps_info.Community.values[0], gps_info.Free.values[0], gps_info.Independent.values[0], gps_info.Other.values[0], gps_info.Budgens.values[0], gps_info.Iceland.values[0],
        gps_info.Lidl.values[0], gps_info['Marks And Spencer'].values[0], gps_info.Morrisons.values[0], gps_info.Sainsburys.values[0], gps_info.Tesco.values[0],
        gps_info['The Co-operative Group'].values[0], gps_info.Waitrose.values[0], 2024,
        property_type_dic[property_type], duration_dic[duration],  total_floor_area, number_rooms,
        current_energy_efficiency, potential_energy_efficiency , month_dic[month_of_transfer],
        average_year, average_price]
        X_new = np.array(X_new).reshape(1, -1)
        y_pred = pipeline.predict(X_new)
        property_data = {  
            "Number of Rooms": number_rooms,
            "Floor area (sq m)": total_floor_area,
            "Property type": property_type, 
            "Closest TfL station (m)": round(gps_info.closest_station_1_distance.values[0]), 
            "Number of schools around half mile": round(gps_info.Academy.values[0]+ 
        gps_info.Community.values[0]+  gps_info.Free.values[0]+ gps_info.Independent.values[0]+ gps_info.Other.values[0]),
            "M&S or Waitrose with in a mile": gps_info['Marks And Spencer'].values[0] + gps_info.Waitrose.values[0],
        }
        # Move to the description page
        property_description_page(postcode, property_data, gps_info, y_pred)

def property_description_page(postcode, property_data, gps_info, y_pred):
    st.title("Property Price")

    # Display Property Information
    st.write("**Postcode:**", postcode)
    for column_name, value in property_data.items():
        st.write(f"**{column_name}:** {value}")

    # GPS Information and Map
    st.subheader("Location")
    latitude, longitude = gps_info.latitude.values[0], gps_info.longitude.values[0] 
    property_location = folium.Map(location=[latitude, longitude], zoom_start=15) 

    # Add a marker
    folium.Marker(
        location=[latitude, longitude],
        popup=postcode 
    ).add_to(property_location)

    # Display the map
    st.subheader("Location")
    folium_static(property_location) 
    
    # Estimated price
    prediction = y_pred[0]
    st.subheader("Estimated Price")
    st.write(f" £ {round(prediction)} ")

    

def find_gps_from_postcode(postcode1, postcode2, gps_data):
    full_postcode = postcode1 + " " + postcode2
    matching_row = gps_data[gps_data['postcode'] == full_postcode]
    return matching_row

def get_avg_price(avg_price, ward):
    return avg_price[avg_price.wards == ward].avg_price.values[0]
def get_average_year(s):
    # Handle special cases
    if s == 'England and Wales: before 1900':
        return 1899  # assumption - it can be older 
    elif s == 'England and Wales: 2007 onwards':
        return 2007  # Assuming an average or specific logic for "onwards" althou it is confusing becasue there is England and Wales: 2007-2011
    elif s == 'England and Wales: 2012 onwards':
        return 2012  # asuming the minimum
    elif s == 'INVALID!':
        return np.nan  # Handle invalid entries to then get rid of the empty entries
    else:
        # Extract years and calculate the average
        years = [int(year) for year in s.split(':')[-1].split('-')]
        return sum(years) / len(years)

if __name__ == "__main__":
    main()