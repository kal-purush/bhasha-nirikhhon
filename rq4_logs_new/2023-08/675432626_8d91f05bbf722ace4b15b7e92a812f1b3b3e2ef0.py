import streamlit as st
import pandas as pd
import plotly_express as px

data = pd.read_csv('adjusted_data.csv')

#Selection of Car Brand
st.header('Used Car Market')
st.write("""Filter the data below to see listed prices by manufacturer""")
show_new_cars = st.checkbox('Include new cars from dealers.', value =True)

#Brand and Condition of Vehicle
show_new_cars

if not show_new_cars:
    data = data[data['condition']!= 'like new']

manufacturer_choice = data['make'].unique()

make_choice = st.selectbox('Select Manufacturer:', manufacturer_choice)

#Range of Vehicle Ages
min_year,max_year = (data['model_year'].min(), data['model_year'].max())
year_range = st.slider(label = 'Choose Year', min_value=min_year, max_value=max_year, value=(min_year, max_year))


actual_range = list(range(year_range[0], year_range[1]+1))


filtered_data = data[(data['make'] == make_choice) & (data['model_year'].isin(list(actual_range)))]
st.table(filtered_data)

#Price Analysis Menu
st.header('Vehicle Price Analysis')
st.write("""Here we can take a look at what influences the price most.  Analyze this by comparing transmission type, vehicle condition, miles on odometer, and days listed on the market""")

histogram_list = ['condition', 'transmission', 'odometer', 'days_listed']
histogram_choice = st.selectbox('Compare for Price Distribution', histogram_list)
histogram1 = px.histogram(data, x='price', color = histogram_choice) 
histogram1.update_layout(title = '<b> Comparison of Price by {}<b>'.format(histogram_choice))
st.plotly_chart(histogram1)
histogram1.show()

#creation of age categories
def age_category(age):
    if (age < 5):
        return '<5'
    elif (age >= 5 and age < 10):
        return '5-10'
    elif (age >= 10 and age < 15):
        return '10-15'
    elif (age >=15 and age < 20):
        return '15-20'
    else:
        return '20+'
data['age_category'] = data['age'].apply(age_category)


#Price Scatterplot Choice
st.write ("""How is the vehicle price affected by number of miles on car, color, or transmission type?""")

scatter_list = ['age', 'odometer', 'age_category']
scatter_choice = st.selectbox ('Price dependency on ', scatter_list)
scatter1 = px.scatter(data, x = 'price', y=scatter_choice, hover_data=['model_year'])
scatter1.update_layout(title = '<b> Price Comparison by {}<b>'.format(scatter_choice))
st.plotly_chart(scatter1)

scatter1.show()

