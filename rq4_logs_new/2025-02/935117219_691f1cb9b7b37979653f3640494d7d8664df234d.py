import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Fetch ES futures data
stock = yf.download("ES=F", start="2020-01-01", end="2025-02-25")
stock = stock.dropna()

# Define SMA windows (40 and 150, as per your graph)
stock["SMA40"] = stock["Close"].rolling(window=40).mean()
stock["SMA150"] = stock["Close"].rolling(window=150).mean()

# Create signals (buy when 40-day SMA > 150-day SMA, sell otherwise)
stock["Signal"] = 0
stock.loc[stock["SMA40"] > stock["SMA150"], "Signal"] = 1  # Buy
stock.loc[stock["SMA40"] < stock["SMA150"], "Signal"] = -1  # Sell

# Calculate daily returns and strategy returns
stock["Returns"] = stock["Close"].pct_change()
stock["Strategy_Returns"] = stock["Returns"] * stock["Signal"].shift(1)
cumulative_pct = (
    (1 + stock["Strategy_Returns"].fillna(0)).cumprod() - 1) * 100

# Focus on 2022–2023 for analysis
start_date = "2022-01-01"
end_date = "2023-12-31"
period_data = stock.loc[start_date:end_date]

# Plot price, SMAs, and signals for the flatline period
plt.figure(figsize=(12, 8))

# Plot 1: Price and SMAs
plt.subplot(2, 1, 1)
plt.plot(period_data.index,
         period_data["Close"], label="Close Price", color="black")
plt.plot(period_data.index,
         period_data["SMA40"], label="40-day SMA", color="blue")
plt.plot(period_data.index,
         period_data["SMA150"], label="150-day SMA", color="red")
plt.title("E-mini S&P 500 Futures (ES=F) 2022–2023: Price and SMAs")
plt.xlabel("Date")
plt.ylabel("Price")
plt.legend()

# Plot 2: Signals and Cumulative Returns
plt.subplot(2, 1, 2)
plt.plot(period_data.index, period_data["Signal"],
         label="Signal (1=Buy, -1=Sell, 0=Hold)", color="purple")
plt.plot(period_data.index,
         cumulative_pct.loc[start_date:end_date], label="Cumulative Return (%)", color="green")
plt.title("Signals and Cumulative Returns (2022–2023)")
plt.xlabel("Date")
plt.ylabel("Value")
plt.legend()

plt.tight_layout()
plt.show()

# Print summary statistics for the period
print("Summary for 2022–2023:")
print(f"Number of Buy Signals: {len(period_data[period_data['Signal'] == 1])}")
print(
    f"Number of Sell Signals: {len(period_data[period_data['Signal'] == -1])}")
print(
    f"Mean Daily Strategy Return: {period_data['Strategy_Returns'].mean() * 100:.4f}%")
print(
    f"Standard Deviation of Daily Returns: {period_data['Strategy_Returns'].std() * 100:.4f}%")