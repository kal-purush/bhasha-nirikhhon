import streamlit as st
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
import folium
from folium.plugins import HeatMap
from streamlit_folium import folium_static

st.title("California House Price Prediction")

df_error = pd.read_csv('models_error.csv')

main_df = pd.read_csv('processed_data.csv')

y = main_df['median_house_value']
X =  main_df.drop(['median_house_value'], axis = 1)

scaler = StandardScaler()
scaler.fit(X)


def user_interface():
    longitude = st.sidebar.slider('Longitude', float(main_df.longitude.min()), float(main_df.longitude.max()),
                                  float(main_df.longitude.mean()))
    latitude = st.sidebar.slider('Latitude', float(main_df.latitude.min()), float(main_df.latitude.max()),
                                 float(main_df.latitude.mean()))
    housing_median_age = st.sidebar.slider('Housing Median Age', float(main_df.housing_median_age.min()),
                                           float(main_df.housing_median_age.max()),
                                           float(main_df.housing_median_age.mean()))
    median_income = st.sidebar.slider('Median Income', float(main_df.median_income.min()),
                                      float(main_df.median_income.max()), float(main_df.median_income.mean()))
    # ocean_proximity = st.sidebar.slider('Ocean proximity', float(main_df.ocean_proximity.min()),
    #                                   float(main_df.ocean_proximity.max()), float(main_df.ocean_proximity.mean()))
    ocean_proximity = st.sidebar.selectbox('Ocean proximity',
                                           ('NEAR BAY', '<1H OCEAN', 'INLAND', 'NEAR OCEAN', 'ISLAND'))
    ocean_proximity_to_float64 = {'NEAR BAY': 0, '<1H OCEAN': 1, 'INLAND': 2, 'NEAR OCEAN': 3, 'ISLAND': 4}
    ocean_proximity = ocean_proximity_to_float64[ocean_proximity]
    count_rooms = st.sidebar.slider('Count rooms', float(main_df.count_rooms.min()), float(main_df.count_rooms.max()),
                                    float(main_df.count_rooms.mean()))
    count_bedrooms = st.sidebar.slider('Count bedrooms', float(main_df.count_bedrooms.min()),
                                       float(main_df.count_bedrooms.max()), float(main_df.count_bedrooms.mean()))
    people_per_house = st.sidebar.slider('People per house', float(main_df.people_pre_house.min()), float(main_df.people_pre_house.max()),
                                   float(main_df.people_pre_house.mean())),


    # longitude = st.number_input('longitude')
    # latitude = st.number_input('latitude')
    # housing_median_age = st.number_input('housing_median_age')
    # median_income = st.number_input('median_income')
    # ocean_proximity = st.number_input('ocean_proximity')
    # count_rooms = st.number_input('count_rooms')
    # count_bedrooms = st.number_input('count_bedrooms')
    # people_per_house = st.number_input('people_per_house')


    data = {'Longitude': longitude,
            'Latitude': latitude,
            'Housing Median Age': housing_median_age,
            'Median Income': median_income,
            'Ocean proximity': ocean_proximity,
            'Count rooms': count_rooms,
            'Count bedrooms': count_bedrooms,
            'People per house': people_per_house}

    features = pd.DataFrame(data, index=[0])
    return features


df = user_interface()

# import matplotlib
# import matplotlib.pyplot as plt
#
# df.plot(kind='scatter', x='Longitude', y='Latitude', alpha=0.4, figsize=(10,7), c = 'red')
#
# california_img = matplotlib.image.imread('california.png')
# plt.imshow(california_img, extent=[-124.55, -113.80, 32.45, 42.05], alpha=0.5)
# plt.ylabel("Latitude", fontsize=14)
# plt.xlabel("Longitude", fontsize=14)
# st.pyplot(plt)


st.header('Input parameters:')
st.table(df)

map_hooray = folium.Map(location=[36.7783,-119.4179],
                    zoom_start = 6, min_zoom=5)

df_map = df[['Latitude', 'Longitude']]
data = [[row['Latitude'],row['Longitude']] for index, row in df_map.iterrows()]
HeatMap(data, radius=10).add_to(map_hooray)
folium_static(map_hooray)

st.header("Prediction")
########## SEQUENTIAL
st.subheader('1. **Sequential model**')
sequential_model = tf.keras.models.load_model('model_sequential')
df = scaler.transform(df)
sequential_price = round(sequential_model.predict(df)[0][0], 2)
st.write(f'Price: {sequential_price}')

######### Deep wide model
st.subheader('2. **Deep wide model**')
deep_wide_model = tf.keras.models.load_model('model_deep_wide')
deep_wide_price = deep_wide_model.predict(df)[0][0]
st.write(f'Price: {round(deep_wide_price, 2)}')

######### 2 inputs model
st.subheader('3. **2 inputs model**')
inputs_model = tf.keras.models.load_model('model_2inputs')
X_A, X_B = df[:, [3, 5, 2]], df[:, [0, 1, 4, 6, 7]]
inputs_price = inputs_model.predict((X_B, X_A))[0][0]
st.write(f'Price: {round(inputs_price, 2)}')

######### 2 outputs model
st.subheader('4. **2 outputs model**')
outputs_model = tf.keras.models.load_model('model_2outputs')
outputs_price = outputs_model.predict((X_B, X_A))[0][0][0]
st.write(f'Price: {outputs_price}')

st.header('Models error:')
df_error.drop(columns='Unnamed: 0', inplace = True)
st.table(df_error)