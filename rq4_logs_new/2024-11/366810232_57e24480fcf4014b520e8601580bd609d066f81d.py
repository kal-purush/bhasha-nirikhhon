import streamlit as st
import pandas


data = {
    'Series 1':[1, 3, 5, 7, 9],
    'Series 2':[10, 20, 30, 40, 50]
}

df = pandas.DataFrame(data)

st.title('Our first Streamlit App')
st.subheader('Introducing Streamlit')
st.write("""
This is our first web app.
Enjoy it!
""")
st.write(df)
st.line_chart(df)
st.area_chart(df)

myslider = st.slider('Celcius')
st.write(myslider, 'in Fahrenheit is', myslider * 9 + 32)