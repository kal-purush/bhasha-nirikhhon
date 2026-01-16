import datetime
from datetime import date
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import psycopg2

default_args = {"owner": "airflow"}

def data_scraper_topgainers(**context):
    top_gainers = pd.read_html('https://www.tradingview.com/markets/stocks-usa/market-movers-gainers/')
    top_gainers = top_gainers[0]
    top_gainers.columns = ["Ticker", "Last", "Change%", "Change", "Rating", "Volume", "Market Cap", "P/E", "EPS(TTM)", "Employees", "Sector"]
    top_gainers["Change%"] = top_gainers['Change%'].str.split("%").str[0]
    top_gainers['Change%'] = top_gainers['Change%'].astype('float64')
    top_gainers["P/E"].replace('—', 0.00, inplace=True)
    top_gainers['P/E'] = top_gainers['P/E'].astype('float64')
    top_gainers['EPS(TTM)'].replace('—', 0.00, inplace=True)
    top_gainers['EPS(TTM)'] = top_gainers['EPS(TTM)'].astype('float64')
    top_gainers['Employees'].replace('—', 0.00, inplace=True)
    top_gainers['Employees'] = top_gainers['Employees'].astype('float64')

    # To Clean Single Quotes
    top_gainers["Ticker"] = top_gainers["Ticker"].str.replace(r"'", r'"', regex=True)

    # Add Date Column
    today = date.today()
    current_date = today.strftime("%m/%d/%Y")

    top_gainers["Dates"] = current_date

    # Connect to your postgres DB
    conn = psycopg2.connect(
            host = "<Your Host>",
            port = "<Your Port>",
            database = "<Your Database>",
            user = "<Your Username>",
            password = "<Your Password>")

    # Open a cursor to perform database operations
    cur = conn.cursor()

    for index, row in top_gainers.iterrows():
        cur.execute("""
        INSERT INTO top_gainers (
        ticker,
        last,
        change_percent,
        change_points,
        rating,
        volume,
        marketcap,
        pe_ratio,
        eps,
        employees, 
        sector,
        dates)
        VALUES (
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}');
        """.format(
        row[0],
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
        row[9],
        row[10],
        row[11])
    )

    # Commit the insert update
    conn.commit()

def data_scraper_toplosers(**context):
    top_losers = pd.read_html('https://www.tradingview.com/markets/stocks-usa/market-movers-losers/')
    top_losers = top_losers[0]
    top_losers.columns = ["Ticker", "Last", "Change%", "Change", "Rating", "Volume", "Market Cap", "P/E", "EPS(TTM)", "Employees", "Sector"]
    top_losers["Change%"] = top_losers['Change%'].str.split("%").str[0]
    top_losers['Change%'] = top_losers['Change%'].astype('float64')
    top_losers["P/E"].replace('—', 0.00, inplace=True)
    top_losers['P/E'] = top_losers['P/E'].astype('float64')
    top_losers['EPS(TTM)'].replace('—', 0.00, inplace=True)
    top_losers['EPS(TTM)'] = top_losers['EPS(TTM)'].astype('float64')
    top_losers['Employees'].replace('—', 0.00, inplace=True)
    top_losers['Employees'] = top_losers['Employees'].astype('float64')

    # To Clean Single Quotes
    top_losers["Ticker"] = top_losers["Ticker"].str.replace(r"'", r'"', regex=True)

    # Add Date Column
    today = date.today()
    current_date = today.strftime("%m/%d/%Y")

    top_losers["Dates"] = current_date

    # Connect to your postgres DB
    conn = psycopg2.connect(
            host = "<Your Host>",
            port = "<Your Port>",
            database = "<Your Database>",
            user = "<Your Username>",
            password = "<Your Password>")

    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    for index, row in top_losers.iterrows():
        cur.execute("""
        INSERT INTO top_losers (
        ticker,
        last,
        change_percent,
        change_points,
        rating,
        volume,
        marketcap,
        pe_ratio,
        eps,
        employees, 
        sector,
        dates)
        VALUES (
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}',
        '{}');
        """.format(
        row[0],
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
        row[9],
        row[10],
        row[11])
    )

    # Commit the insert update
    conn.commit()


with DAG(
    dag_id="Stock_Gainers_Losers",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    step1 = PythonOperator(
        task_id='Step_1',
        python_callable=data_scraper_topgainers,
        provide_context=True
    )

    step2 = PythonOperator(
        task_id='Step_2',
        python_callable=data_scraper_toplosers,
        provide_context=True
    )

#     step2 = PostgresOperator(
#     task_id="Step_2: Create Stock Gainers Table",
#     postgres_conn_id="postgres_default",
#     sql="""
#         CREATE TABLE IF NOT EXISTS stock_gainers (
#         ticker VARCHAR PRIMARY KEY,
#         last DECIMAL,
#         change_percent DECIMAL,
#         change_points DECIMAL,
#         rating VARCHAR,
#         volume VARCHAR,
#         marketcap VARCHAR,
#         pe_ratio DECIMAL,
#         eps DECIMAL,
#         employees DECIMAL, 
#         sector VARCHAR);
#         """
# )   




step1 >> step2