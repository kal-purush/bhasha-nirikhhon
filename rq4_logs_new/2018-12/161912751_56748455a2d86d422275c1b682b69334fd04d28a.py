import dash
from dash.dependencies import Output, Event, Input
import dash_core_components as dcc
import dash_html_components as html
from random import random
import plotly
import csv
import os #os module imported here
import time
# import pandas as pd
html.Div(id='run-log-storage', style={'display': 'none'}),


def get_latest(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.csv')]
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return files[0]


def file_to_analizes():
    time_sorted_list = None
    try:
        folder_path = r'D:\STUDIA\Inżynierka\testowy'
        time_sorted_list = get_latest(folder_path)

        # except FileNotFoundError:
        #     print('bład')
    except IndexError:
        print('blad')
    if time_sorted_list is None:
        plots = None
    else:
        csv_file = open(time_sorted_list)
        plots = csv.reader(csv_file, delimiter=';')
    return plots


app = dash.Dash(__name__)
app.config['suppress_callback_exceptions']=True
app.layout = html.Div([
    html.Div([
        html.H1("Simple input example"),

        html.Div(id='result'),
        dcc.Graph(id='live-update-graph-scatter', animate=True),
        # dcc.Graph(id='live-update-graph-bar'),
        dcc.Interval(
            id='interval-component',
            interval=1*2000
        )
    ]),
    html.Div([
        html.Div([
            dcc.Dropdown(
                id='dropdown-interval-control',
                options=[
                    {'label': 'No Updates', 'value': 'no'},
                    {'label': 'Slow Updates', 'value': 'slow'},
                    {'label': 'Regular Updates', 'value': 'regular'},
                    {'label': 'Fast Updates', 'value': 'fast'}
                ],
                value='regular',
                className='ten columns',
                clearable=False,
                searchable=False
            ),
            ]),
        dcc.Interval(
            id="interval-log-update",
            n_intervals=0
        ),
]),
])

# @app.callback(
#     Output('result', 'children'),
#     event= [Event('interval-component', 'interval')]
#     [Input('input-x', 'value')]
# )
# def update_result(x):
#     y=update_graph_scatter
#     return "The sum is: {}".format(y)


@app.callback(Output('interval-log-update', 'interval'),
              [Input('dropdown-interval-control', 'value')])
def update_interval_log_update(interval_rate):
    if interval_rate == 'fast':
        return 500

    elif interval_rate == 'regular':
        return 1000

    elif interval_rate == 'slow':
        return 5 * 1000

    # Refreshes every 24 hours
    elif interval_rate == 'no':
        return 24 * 60 * 60 * 1000


@app.callback(Output('result', 'children'),
              [Input('interval-log-update', 'n_intervals')])
def update_div_current_accuracy_value(file_to_analizes):

        x=get_latest(r'D:\STUDIA\Inżynierka\testowy')

        return [
            html.P(
                "Current Accuracy:",
                style={
                    'font-weight': 'bold',
                    'margin-top': '15px',
                    'margin-bottom': '0px'
                }
            ),
            html.Div(x),
        ]


@app.callback(Output('live-update-graph-scatter', 'figure'),
              events=[Event('interval-component', 'interval')])
def update_graph_scatter():
    x = []
    y = []
    line_count = 0
    plots = file_to_analizes()
    print(plots)
    traces = list()
    traces.clear()
    if plots is not None:
        for row in plots:
            if line_count < 280:
                line_count += 1
            else:
                x.append(float(row[1]))
                y.append(float(row[2]))
                line_count += 1
        traces.append(plotly.graph_objs.Scatter(
            x=x,
            y=y,
            name='Scatter {}'.format(1),
            mode='lines+markers'
            ))
        return {'data': traces}
    # elif plots is None:
    #     traces.append(plotly.graph_objs.Scatter(
    #     x=[],
    #     y=[],
    #     name='Scatter {}'.format(1),
    #     mode='lines+markers'))
    #     return {'data': traces}

# @app.callback(Output('live-update-graph-bar', 'figure'),
#               events=[Event('interval-component', 'interval')])
# def update_graph_bar():
#
#     traces = list()
#     for t in range(2):
#         traces.append(plotly.graph_objs.Bar(
#             x=[1, 2, 3, 4, 5],
#             y=[(t + 1) * random() for i in range(5)],
#             name='Bar {}'.format(t)
#             ))
#     layout = plotly.graph_objs.Layout(
#     barmode='group'
# )
#     return {'data': traces, 'layout': layout}


if __name__ == '__main__':
    app.run_server(debug=True)