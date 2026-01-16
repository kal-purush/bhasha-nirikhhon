import dash
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
import pickle as pkl
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.express as px


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# Change the file path
with open('xgmodel.pkl', 'rb') as f:
        model = pkl.load(f)

app = dash.Dash(__name__,external_stylesheets=[dbc.themes.COSMO], title='Fraud Detector')

app.layout = html.Div(children=[
    dbc.NavbarSimple(brand='House Sale Price Predictor', color='pink'),
    
    html.H5(children=' * NOTICE: 1 is High Risk, 0 is low risk'),
    html.Br(),
    
    'Approximate Payout Date: ',
    dcc.Input(id='my-input1', value=1296720000, type='number',style={'marginRight':'5px'}),
    html.Br(),
    html.Br(),
    
    'Sale Duration',
    dcc.Input(id='my-input2', value=28, type='number'),
    html.Br(),
    html.Br(),
    
    'User Age',
    dcc.Input(id='my-input3', value=149, type='number'),
    html.Br(),
    html.Br(),
    
    'User Type',
    dcc.Input(id='my-input4', value=3, type='number'),
    html.Br(),
    html.Br(),
    
    'Event Length',
    dcc.Input(id='my-input5', value=32400, type='number'),
    html.Br(),
    html.Br(),
    
    
    html.Div(id='my-output')
])


@app.callback(
    Output("my-output", "children"),
    Input("my-input1", "value"),
    Input("my-input2", "value"),
    Input("my-input3", "value"),
    Input("my-input4", "value"),
    Input("my-input5", "value"),
)
def prediction(inp1,inp2,inp3,inp4,inp5):
    p = model.predict([[inp1,inp2,inp3,inp4,inp5]])
    return "Result: {}".format(p)


if __name__ == '__main__':
    app.run_server(debug=True)
    
#     rf_df= 
#  'approx_payout_date','sale_duration2','user_age','user_type','event_len','label'
# df[['approx_payout_date','fb_published','sale_duration2','has_analytics','user_age','user_type','event_len','label','show_map']]