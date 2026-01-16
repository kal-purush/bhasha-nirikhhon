from dash import Dash, html, dcc, callback, Output, Input
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
import dash_daq as daq


# Load data
file = 'fund_241229.csv'
path = 'pages'
df_prc = pd.read_csv(
    f'{path}/{file}',
    parse_dates=['date'],
    dtype={'ticker': str},
    index_col=['group', 'ticker', 'date']
)

groups = df_prc.index.get_level_values('group').unique()
default_group = 2030
groups = [{'label': f'TDF{x}', 'value': x} for x in groups]

# Initialize the Dash app
external_stylesheets = [dbc.themes.CERULEAN]
app = Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(
                id='group-dropdown',
                options=groups,
                value=default_group,
                clearable=False,
            ),
            width=3  # Adjust width as needed
        ),
        dbc.Col(
                daq.BooleanSwitch(
                    id='compare-boolean-switch',
                    on=False
                ),
                width="auto"),
        dbc.Col(
                daq.BooleanSwitch(
                    id='cost-boolean-switch',
                    on=False
                ),
                width="auto"),
        ], 
        justify="center",  # Centers the row's content horizontally
        align="center", # Centers the row's content vertically
        className="mb-3" # Bootstrap margin-bottom class
        #style={'margin-bottom': '20px'}  # Adds space below the dropdown row
    ),
    dbc.Row(dcc.Graph(id='price-plot')),
    dbc.Row(dcc.Graph(id='return-plot')),
    # Store DataFrame in JSON format
    dcc.Store(id='price-data'),
    dbc.Tooltip(
        '상대 비교',
        target='compare-boolean-switch',
        placement='bottom'
    ),
    dbc.Tooltip(
        '비용 고려',
        target='cost-boolean-switch',
        placement='bottom'
    )
])


def get_group_data(group, col, start=None, base=1000):
    df = df_prc.loc[group, col].unstack('ticker').sort_index()
    default = {
        'price': df.to_dict('records'),
        'index': df.index.tolist(),
    }
    start = df.apply(lambda x: x[x.notna()].index.min()).max() if start is None else start
    normalized_df = df.apply(lambda x: x / x.loc[start] * base).loc[start:]
    compare = {
        'price': normalized_df.to_dict('records'),
        'index': normalized_df.index.tolist(),
    }
    return {'default': default, 'compare': compare}, start


@app.callback(
    Output('price-data', 'data'),
    Input('group-dropdown', 'value'),
)
def update_price_data(group, base=1000):
    """
    process data and save to dcc.Store
    """
    cols = df_prc.columns
    data = {'columns':cols, 'default':dict(), 'compare':dict()}
    
    start = None
    for col in cols:
        col_data, start = get_group_data(group, col, start, base)
        data['default'][col] = col_data['default']
        data['compare'][col] = col_data['compare']
    return data


def get_title(title, compare, cost):
    if compare:
        title_comp = '상대 가격'
    else:
        title_comp = '각 최종 결산 기준'

    if cost:
        title_cost = '비용 고려'
    else:
        title_cost = None

    title = f'{title} ({title_comp}'
    title = f'{title}, {title_cost})' if title_cost else f'{title})'
    return title


@app.callback(
    Output('price-plot', 'figure'),
    Input('price-data', 'data'),
    Input('cost-boolean-switch', 'on'),
    Input('compare-boolean-switch', 'on')
)
def update_price_plot(data, cost, compare):
    cols = data['columns']
    col = cols[1] if cost else cols[0]
    kind = 'compare' if compare else 'default'
    dat = data[kind][col]
    df = pd.DataFrame(dat['price'], index=dat['index'])
    title = get_title('펀드 가격 추이', compare, cost)
    return px.line(df, title=title, height=300)


@app.callback(
    Output('return-plot', 'figure'),
    Input('price-data', 'data'),
    Input('cost-boolean-switch', 'on'),
    Input('compare-boolean-switch', 'on')
)
def update_return_plot(data, cost, compare, date_format='%Y-%m-%d'):
    cols = data['columns']
    # reset opacity of selected price data
    sel = cols[1] if cost else cols[0]
    if compare:
        dat = data['compare']
        dates = pd.Index(dat[sel]['index'])
        dates = pd.to_datetime(dates)
        dt0 = dates.min().strftime(date_format)
        dt1 = dates.max().strftime(date_format)
        title = f'펀드 수익률 ({dt0} ~ {dt1})'
    else:
        dat = data['default'] 
        title = '펀드 수익률 (각 설정일 기준)'

    df_ret = pd.DataFrame()
    for col in cols:
        prc = dat[col]['price']
        df = pd.DataFrame(prc)
        df = df.apply(lambda x: x.dropna().iloc[-1]/x.dropna().iloc[0]-1).to_frame(col)
        df_ret = pd.concat([df_ret, df], axis=1)
    fig = px.bar(df_ret, title=title, barmode='group', opacity=0.3)
    fig.update_traces(marker_opacity=1, marker_line_color="black", selector={"name": sel})
    return fig


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)