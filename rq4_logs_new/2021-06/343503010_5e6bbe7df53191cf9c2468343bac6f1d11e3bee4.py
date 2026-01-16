import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV
from scipy.stats import randint


#####
###   This function loads the data file housing.csv.zip into a dataframe.
###   Return type: DataFrame
#####
@st.cache
def load_data():
	raw_housing_price = pd.read_csv('housing.csv.zip')

	# Drop total_bedrooms, because the correlation is only 0.049686.
	cleaned_housing_price = raw_housing_price.drop('total_bedrooms', axis = 1)
	# Drop longitude(-0.0459), population(-0.0246), and households(0.065843)
	cleaned_housing_price = cleaned_housing_price.drop(['longitude', 'population', 'households'], axis=1)
	return cleaned_housing_price


#####
###   This function trains the model on training_data.
###   Return Type: model
#####
@st.cache(allow_output_mutation=True)
def train_model(training_data):
	### Convert column ocean_proximity from string categories to int.
	training_data = training_data.replace({'ISLAND':0, 'NEAR BAY':1, 'NEAR OCEAN':2, '<1H OCEAN':3, 'INLAND':4})
	### Drop target.
	X_train = training_data.drop(['median_house_value'], axis=1)
	y_train = training_data.median_house_value

	param_distribs = {
        	'n_estimators': randint(low=1, high=200),
        	'max_features': randint(low=1, high=5),
		}

	forest_model = RandomForestRegressor(random_state=42)
	rnd_search = RandomizedSearchCV(forest_model, param_distributions=param_distribs,
		n_iter=10, cv=5, scoring='neg_mean_squared_error', random_state=42)
	rnd_search.fit(X_train, y_train)
	return rnd_search.best_estimator_


#####
###   This function is used to get user input using sidebar slider and selectbox
###   Return type: pandas dataframe
#####
def get_user_input(data):

	### ocean proximity - select box with string categories.
	data_ocean_proximity = data['ocean_proximity']
	ocean_proximity = st.sidebar.selectbox("Select Ocean Proximity", list(set(data_ocean_proximity)))

	### latitude - slider with min, max, and mean
	data_latitude = data['latitude']
	latitude = st.sidebar.slider('Latitude', data_latitude.min(), data_latitude.max(), data_latitude.mean())

	### housing_median_age - slider with min, max, and mean
	data_median_age = data['housing_median_age']
	housing_median_age = st.sidebar.slider('Housing Median Age', data_median_age.min(), data_median_age.max(), data_median_age.mean())

	### total rooms - slider with min, max, and mean
	data_total_rooms = data['total_rooms']
	total_rooms = st.sidebar.slider('Total Rooms', data_total_rooms.min(), data_total_rooms.max(), data_total_rooms.mean())
	
	### median income - slider with min, max, and mean
	data_median_income = data['median_income']
	median_income = st.sidebar.slider('Median Income', data_median_income.min(), data_median_income.max(), data_median_income.mean())

	features = {'ocean_proximity': ocean_proximity,
		'latitude': latitude,
		'housing_median_age': housing_median_age,
		'total_rooms': total_rooms,
		'median income': median_income
		}
	data = pd.DataFrame(features, index=[3])
	return data


### Web Page Title
st.title('California Housing Prices')


### Display loading info.
data_load_state = st.text('Loading data...')
data = load_data()
data_load_state.text("")


### If checkbox is selected, list loaded data.
if st.checkbox('Show raw data'):
	st.subheader('Raw data')
	st.write(data)


### Display Training Model info.
data_load_state = st.text('Training model...')
model = train_model(data)
data_load_state.text("")


### Display histogram for median_house_value.
st.subheader('Median House Values')
hist_values = np.histogram(data['median_house_value'], bins=150, range=(0,500000))[0]
st.bar_chart(hist_values)


### Display Altari Chart of median_income vs median_house_value.
data_median_income = data['median_income']
filtered_data = data[data_median_income.between(data_median_income.min(), data_median_income.max())]
ticks = [float(x)/10 for x in range(0, int(data_median_income.max()), 1)]
chart = alt.Chart(filtered_data).mark_point().encode(
	x = alt.X('median_income', axis = alt.Axis(values=ticks)),
	y = 'median_house_value',
	color = 'ocean_proximity',
	tooltip = ['latitude', 'housing_median_age', 'total_rooms', 'median_house_value']
).interactive()
st.altair_chart(chart)


### Gather user info.
st.subheader('User Input parameters')
user_input = get_user_input(data)
st.write(user_input)


### Predicted medain house value.
st.subheader('Predicted Median House Value:')
user_input = user_input.replace({'ISLAND':0, 'NEAR BAY':1, 'NEAR OCEAN':2, '<1H OCEAN':3, 'INLAND':4})
final_model = model.predict(user_input)
st.write(final_model)