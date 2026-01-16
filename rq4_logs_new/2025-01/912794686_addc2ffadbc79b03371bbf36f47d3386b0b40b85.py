import os
import importlib
import inspect
import backtrader as bt
import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import io
import time
import random
import openpyxl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import base64
from Strategies.buy_and_hold import BuyAndHold

# Define folder paths
TICKERS_CSV_PATH = './Tickers/tickers.csv'
STRATEGIES_PATH = './Strategies'

# Read tickers from CSV
tickers_df = pd.read_csv(TICKERS_CSV_PATH)

def load_strategies():
    strategies = {}
    for filename in os.listdir(STRATEGIES_PATH):
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]  # Remove '.py'
            module = importlib.import_module(f'Strategies.{module_name}')
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, bt.Strategy) and obj != bt.Strategy:
                    strategies[name] = obj
    return strategies

def run_backtest(data, strategy_class, start_cash=10000.0, commission=0.001):
    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.addstrategy(strategy_class)
    cerebro.broker.setcash(start_cash)
    cerebro.broker.setcommission(commission=commission)
    strategies = cerebro.run(runonce=False)
    final_value = cerebro.broker.getvalue()
    strategy = strategies[0]
    trade_count = strategy.order_count if hasattr(strategy, 'order_count') else 0
    current_signal = strategy.signal if hasattr(strategy, 'signal') else None
    roi = strategy.roi if hasattr(strategy, 'roi') else ((final_value / start_cash) - 1.0)
    return final_value, trade_count, current_signal, roi

#Function to compare the strategy to buy and hold
def calculate_buy_and_hold(data, start_cash=10000.0):
    initial_price = data['Close'].iloc[0]
    final_price = data['Close'].iloc[-1]
    shares = start_cash / initial_price
    final_value = shares * final_price
    return final_value


def fetch_data_with_retry(ticker, start_date, end_date, max_retries=5):
    for attempt in range(max_retries):
        try:
            df = yf.download(ticker, start=start_date, end=end_date)
            if not df.empty:
                st.write(f"Data fetched for {ticker}: from {df.index[0]} to {df.index[-1]}")
                return df
        except Exception as e:
            if attempt < max_retries - 1:
                st.warning(f"Attempt {attempt + 1} failed for {ticker}: {str(e)}. Retrying...")
                time.sleep(random.uniform(1, 5))  # Increased max delay time
            else:
                st.error(f"Failed to fetch data for {ticker} after {max_retries} attempts: {str(e)}")
    return pd.DataFrame()

# Streamlit app
st.set_page_config(layout="wide")  # Set the page to wide mode
st.title('Dutch Stock Strategy Backtester')

# User inputs
start_cash = st.number_input('Starting Capital (EUR)', min_value=1000, value=10000, step=1000)
commission = st.number_input('Commission (fraction)', min_value=0.0, max_value=0.1, value=0.001, step=0.001, format="%.3f")

# Date range selection
end_date = st.date_input('End Date', value=datetime.now() + timedelta(days=1))
start_date = st.date_input('Start Date', value=end_date - timedelta(days=365))

# Load all strategies
all_strategies = load_strategies()

results = []
all_start_dates = []
all_end_dates = []
progress_bar = st.progress(0)
for index, row in tickers_df.iterrows():
    ticker = row['Ticker']
    name = row['Name']
    
    # Fetch data with retry
    df = fetch_data_with_retry(ticker, start_date, end_date)
    
    if not df.empty:
        all_start_dates.append(df.index[0])
        all_end_dates.append(df.index[-1])
        data = bt.feeds.PandasData(dataname=df)

        # Calculate init and latest price to give a feeling of stock movement
        initial_price = df['Close'].iloc[0]
        final_price = df['Close'].iloc[-1]

        # Run Buy and Hold strategy
        bh_final_value, bh_trade_count, bh_signal, bh_roi = run_backtest(data, BuyAndHold, start_cash, commission)
        bh_profit = bh_final_value - start_cash
        bh_profit_percentage = bh_roi * 100
        
        for strat_name, strategy_class in all_strategies.items():
            try:
                final_value, trade_count, current_signal, roi = run_backtest(data, strategy_class, start_cash, commission)
                profit = final_value - start_cash
                profit_percentage = roi * 100
                
                # Calculate the difference from Buy and Hold
                profit_corrected = profit_percentage - bh_profit_percentage
                
                results.append({
                    'Ticker': ticker,
                    'Name': name,
                    'Initial Price': initial_price,
                    'Final Price': final_price,
                    'Strategy': strat_name,
                    'Final Value (EUR)': round(final_value, 2),
                    'Profit (EUR)': round(profit, 2),
                    'Profit (%)': round(profit_percentage, 2),
                    'Profit_corrected for B&H (%)': round(profit_corrected, 2),
                    'Start Date': df.index[0].strftime('%Y-%m-%d'),
                    'End Date': df.index[-1].strftime('%Y-%m-%d'),
                    'Trades': trade_count,
                    'Buy/Sell Signal': current_signal
                })
            except Exception as e:
                st.error(f"Error processing {ticker} with strategy {strat_name}: {str(e)}")
    
    # Update progress bar
    progress_bar.progress((index + 1) / len(tickers_df))

# Display results
results_df = pd.DataFrame(results)
st.dataframe(results_df, use_container_width=True)

# Download buttons
if not results_df.empty:
    csv = results_df.to_csv(index=False).encode('utf-8')

    col1, col2 = st.columns(2)

    with col1:
        st.download_button(
            label="Download as CSV",
            data=csv,
            file_name="backtesting_results.csv",
            mime="text/csv"
        )

    with col2:
        excel_buffer = io.BytesIO()
        results_df.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_data = excel_buffer.getvalue()
        
        st.download_button(
            label="Download as Excel",
            data=excel_data,
            file_name="backtesting_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # Display the actual date range of the data
    if all_start_dates and all_end_dates:
        overall_start = min(all_start_dates)
        overall_end = max(all_end_dates)
        st.write(f"Overall data range: from {overall_start.strftime('%Y-%m-%d')} to {overall_end.strftime('%Y-%m-%d')}")
    else:
        st.write("No valid date range found in the results.")
else:
    st.warning("No results to display or download.")

# Display some statistics about the data
st.write(f"Number of tickers processed: {len(set(results_df['Ticker']))}")
st.write(f"Number of strategies applied: {len(set(results_df['Strategy']))}")