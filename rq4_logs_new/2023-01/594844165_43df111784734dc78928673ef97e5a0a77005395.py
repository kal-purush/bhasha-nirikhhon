import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st


df = pd.read_csv('data/inspection-scores.csv')
df.sort_values(by='Restaurant Name', inplace=True)
df.reset_index(drop=True, inplace=True)
df['name & address'] = df['Restaurant Name'] + ' ' + df['Address']
customdata = np.stack((df['Restaurant Name'], df['Score'], df['Address'], df['Inspection Date']),  axis=-1)

# st.dataframe(df)
st.title("Austin Restaurants - Inspection Score")

plot_spot = st.empty()

fig = px.scatter_mapbox(df,
                        lon=df['new_longitude'],
                        lat=df['new_latitude'],
                        zoom=10,
                        color=df['Score'],
                        text=df['Restaurant Name'],
                        color_continuous_scale=['red', 'green'],
                        hover_data=['Restaurant Name']
)
fig.update_layout(mapbox_style='open-street-map')
fig.update_layout(showlegend=False)
fig.update_traces(customdata=customdata, hovertemplate='Name: %{customdata[0]}<br>Score: %{customdata[1]}</br>Address: %{customdata[2]}<br>Inspection Date: %{customdata[3]}</br>')


selection = st.selectbox('Choose Restaurant', options=df['name & address'])

st.dataframe(df[df['name & address'] == selection][['Restaurant Name', 'Score', 'Inspection Date']])
latt=df[df['name & address'] == selection]['new_latitude'].values[0]
lonn=df[df['name & address'] == selection]['new_longitude'].values[0]


if st.button("Center Map"):
    fig.update_layout(mapbox_center=dict(lat=latt, lon=lonn), mapbox_zoom=17)

with plot_spot:
    st.plotly_chart(fig)