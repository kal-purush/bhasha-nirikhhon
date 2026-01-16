from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup

app = Dash(__name__)

connection = sqlite3.connect("hr")

employees = pd.read_sql("SELECT * FROM employees",connection)
jobs=pd.read_sql('SELECT * FROM jobs',connection)
jobs.drop(axis=0,index=0,inplace=True)
jobs["Salary difference"] = jobs["max_salary"] - jobs["min_salary"]
print(jobs)


having_same_job = pd.merge(jobs,employees,how='inner', on='job_id')
bar_dict=having_same_job['job_title'].value_counts()
x=[bar_dict.keys()[i] for i in range(len(bar_dict))]
heights=[bar_dict[i] for i in range(len(bar_dict))]


def clean_column(element_of_column):
    if element_of_column == "-":
        return 0
    else:
        element_of_column = element_of_column[1:]
        element_of_column = element_of_column.replace(",","")
        return float(element_of_column)

def scrape_data():
    global hdds
    URL = "https://www.itjobswatch.co.uk/jobs/uk/sqlite.do"
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'html5lib')
    table = soup.find('table', attrs={'class': 'summary'})
    table.find('form').decompose()
    table_data = table.tbody.find_all("tr")
    table = []
    for i in table_data:
        row = []
        rrr = i.find_all("td")
        if len(rrr) == 0:
            rrr = i.find_all("th")
        for j in rrr:
            row.append(j.text)
        table.append(row)

    hd = table[1]
    hd[0] = "index"
    employee_sal = pd.read_sql("SELECT employees.salary " +
                               "FROM employees", connection)
    avg_salary = employee_sal['salary'].mean()
    df = pd.DataFrame(table)
    df.drop(index=[0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 14, 15], axis=0, inplace=True)
    df.columns = hd
    df.set_index("index", inplace=True)
    df.reset_index(inplace=True)
    
    hdds = df.columns.tolist()[1:]

    for i in hdds:
        df[i] = df[i].apply(clean_column)

    df.loc[4] = ['Average', avg_salary, avg_salary, avg_salary]

    return df


scrape_dataframe = scrape_data()

x_axis = scrape_dataframe["index"]
scrape_dataframe.drop("index",inplace=True,axis=1)
option_years = scrape_dataframe.columns

app.layout = html.Div([
    html.Header(children=[
        html.H4('Job Titles Vs Empolyees'),
    ]),
    dcc.Dropdown(
        id="dropdown",
        options=x,
        value="",
        multi=True,
        placeholder="For all jobs",
    ),
    dcc.Graph(id="graph"),
    
    html.Div([
        html.Div("Minimum"),
        dcc.Slider(0, 30000, 1000,
                   value=1000,
                   id='minimum'
                   ),]),
    html.Div([
        html.Div("Maximum"),
        dcc.Slider(0, 30000, 1000,
                   value=20000,
                   id='maximum'
                   ),]),


    dcc.Graph(id="graph2"),


    
    html.Div("Choose a Year:   "),
    dcc.Dropdown(option_years,
                 value=option_years[0],
                 placeholder=option_years[0],
                 id="years"
                 ),
        
    dcc.Graph(id="graph3"),
],className="allContainter")

@app.callback(
    Output("graph", "figure"),
    Input("dropdown", "value"))

def update_bar_chart(job):
    print("before the function")
    
    data=pd.DataFrame(columns=['job_title','empolyees'])
    data['job_title']=x
    data['empolyees']=heights 
    print(job)
    print(len(job))
    if len(job) > 0:
        data = data[data["job_title"].isin(job)]

    
    # fig = px.bar(x=data['job_title'], y=data['empolyees'])
    fig = go.Figure()
    fig.add_trace(go.Bar(x=data["job_title"], y=data["empolyees"])
                  )
    return fig

@app.callback(
    Output("graph2", "figure"),
    # Output("graph2", "figure2"),
    [
    Input("minimum", "value"),
    Input("maximum", "value"),])

def update_bar_chart(min_val,max_val):
    diff = max_val - min_val
    jobs_new = jobs[jobs["Salary difference"] <= diff]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=jobs_new["Salary difference"], y=jobs_new["job_title"],orientation="h")
                  )
    return fig


@app.callback(
    Output("graph3", "figure"),
    Input("years", "value"),
    )

def update_bar_chart(timeframe):
    if timeframe is None:
        timeframe = option_years[0]
    
    y = scrape_dataframe[timeframe]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x_axis, y=y)
                  )
    return fig



app.run_server(debug=True)