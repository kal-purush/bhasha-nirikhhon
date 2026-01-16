import pandas as pd
import numpy as np
import json
import cufflinks as cf
cf.go_offline()
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
df=pd.read_csv('covid_19_india.csv')
data=dict(type='scattergeo',
        colorscale = 'Viridis',
        reversescale = True,
          locations=df['State/UnionTerritory'],
         locationmode='ISO-3',
          z=df['ConfirmedIndianNational'],
          text=df['Date'],
          colorbar={'title':'Covid-19 cases'})
layout = dict(title = 'Covid-19 victims of India',
              geo = dict(
                  scope='india'
                  )
              )
choromap6=go.Figure(data=[data],layout=layout)
iplot(choromap6)