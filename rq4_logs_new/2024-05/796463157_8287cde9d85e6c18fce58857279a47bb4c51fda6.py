import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from datetime import datetime


# Function to fetch and calculate MACD
def fetch_and_calculate_macd(stock_symbol, start_date=None):
    if start_date is None:
        start_date = datetime.now() - pd.DateOffset(years=3)
    end_date = datetime.now()
    df = yf.download(stock_symbol, start=start_date, end=end_date)
    df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = df['EMA12'] - df['EMA26']
    df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    return df


def identify_macd_crosses(df):
    df['Cross'] = ((df['MACD'].shift(1) < df['Signal'].shift(1)) & (df['MACD'] >= df['Signal']) & (df['MACD'] < 0))
    return df


def post_cross_performance(df):
    crosses = df[df['Cross']].index
    performance = []
    for date in crosses:
        end_date = date + pd.DateOffset(days=10)
        if end_date > df.index[-1]:
            continue  # Skip if the range exceeds the data
        window = df.loc[date:end_date]
        performance.append({
            'Date': date.strftime('%Y-%m-%d'),  # Format the date
            'Close at Cross': df.at[date, 'Close'],
            'High After Cross': window['Close'].max(),
            'Low After Cross': window['Close'].min(),
            'Close After 10 Days': window['Close'].iloc[-1],
            'Percentage Change': ((window['Close'].iloc[-1] - df.at[date, 'Close']) / df.at[date, 'Close']) * 100
        })
    performance_df = pd.DataFrame(performance)
    performance_df.sort_values(by='Date', ascending=False, inplace=True)
    return performance_df.head(5)


# Streamlit UI
st.title('MACD Analyzer')
stock_symbol = st.sidebar.text_input('Enter stock symbol', value='AAPL')

if st.sidebar.button('Analyze'):
    df = fetch_and_calculate_macd(stock_symbol)
    df = identify_macd_crosses(df)
    post_cross_data = post_cross_performance(df)

    # Plotting both price and MACD
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1,
                        subplot_titles=('Stock Price', 'MACD and Signal Line'))

    fig.add_trace(go.Scatter(x=df.index, y=df['Close'], name='Close'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MACD'], name='MACD', line=dict(color='blue')), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['Signal'], name='Signal', line=dict(color='red')), row=2, col=1)

    fig.update_layout(height=600, width=700, title_text="Stock Analysis",
                      template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

    # Display last 5 MACD crossovers with post cross performance
    if not post_cross_data.empty:
        st.write('Last 5 MACD Crossovers (Below 0) and Subsequent 10-day Performance:')
        st.dataframe(post_cross_data)

# To run Streamlit: save this script and run 'streamlit run your_script_name.py' in your terminal.