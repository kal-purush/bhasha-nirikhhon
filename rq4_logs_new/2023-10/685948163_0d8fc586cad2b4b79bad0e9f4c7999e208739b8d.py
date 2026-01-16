import numpy as np
import pandas as pd
from statsmodels.datasets import get_rdataset
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

iris = get_rdataset('iris').data

app = Dash(__name__)

app.layout = html.Div([
    html.H4('Iris samples filtered by petal width'),
    dcc.Graph(id = "graph"),
    html.P("Petal Width : "),
    dcc.RangeSlider(
        id = 'range-slider',
        min = 0, max = 2.5, step = 0.1,
        marks = {0: '0', 2.5: '2.5'},
        value = [0.5, 2]
    ),
])

@app.callback(
    Output("graph", "figure"),
    Input("range-slider", "value"))

def update_bar_chart(slider_range):
    df = iris
    low, high = slider_range

    fig = px.scatter_3d(df,
                        x = 'Sepal.Width',
                        y = 'Sepal.Length',
                        z = 'Petal.Length',
                        color = 'Species')
    return fig

if __name__ == '__main__' : app.run_server(debug=True)