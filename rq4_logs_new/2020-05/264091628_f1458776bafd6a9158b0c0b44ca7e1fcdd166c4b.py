import base64
import datetime
import io

import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table

import pandas as pd
import numpy as np
from pandas import util


#define global df object
global df
df = pd.read_csv("mpg.csv")

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.Div(id='data-upload', style={'display':'none'}),
 
    html.Div([
        html.Div([
            html.B('Column Name:'),
            dcc.Dropdown(id='col_names')
        ], className="six columns"),

        html.Div([
            html.B('Column Value:'),
            dcc.Dropdown(id='col_content')
        ], className="six columns"),
    ], className="row"),

#    dcc.Dropdown(id='col_names'),
#    dcc.Dropdown(id='col_content'),
    html.Div(id='output-data-upload'),
])


def parse_contents(contents, filename, date):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])
    return df

def print_df(col_content, col_names):
    global df
    df1 = df[df[col_names] == col_content]
    #print(col_content)
    #print(col_names)
    return html.Div([
        dash_table.DataTable(
            data=df1.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df1.columns]
        ),

        html.Hr(),  # horizontal line
    ])

@app.callback(Output('data-upload', 'children'),
              [Input('upload-data', 'contents')],
              [State('upload-data', 'filename'),
               State('upload-data', 'last_modified')])
def update_output(list_of_contents, list_of_names, list_of_dates):
    global df
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        df = children[0]
        return

@app.callback(
    Output('col_names', 'options'),
    [Input('data-upload', 'children')])
def set_col_options(dummy):
    global df
    #print(df)
    return [{'label': i, 'value': i} for i in df.columns]

@app.callback(
    Output('col_names', 'value'),
    [Input('col_names', 'options')])
def set_col_value(available_options):
    global df
    #print(df)
    return available_options[0]['value']

@app.callback(
    Output('col_content', 'options'),
    [Input('col_names', 'value')])
def set_col_content_option(col_name):
    global df
    column_list = df[col_name].tolist()
    column_list = np.unique(column_list)
    #print(column_list)
    #print(df)
    return [{'label': i, 'value': i} for i in column_list]

@app.callback(
    Output('col_content', 'value'),
    [Input('col_content', 'options')])
def set_col_options_content_value(available_options):
    global df
    #print(df)
    return available_options[0]['value']

@app.callback(
    Output('output-data-upload', 'children'),
    [Input('col_content', 'value'),
    Input('col_names', 'value')])
def update_table(col_content, col_names):
    global df
    return print_df(col_content, col_names)

if __name__ == '__main__':
    app.run_server(debug=True)