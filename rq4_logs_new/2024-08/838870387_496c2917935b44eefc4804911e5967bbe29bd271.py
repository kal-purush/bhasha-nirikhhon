import yfinance as yf
from datetime import datetime, timedelta

# Function to calculate the percentage difference between today's low and previous day's close
def calculate_percentage_difference(data):
    data['Prev Close'] = data['Close'].shift(1)
    data.dropna(inplace=True)
    data['Pct Difference'] = ((data['Low'] - data['Prev Close']) / data['Prev Close']) * 100
    return data['Pct Difference'].min(), data['Pct Difference'].max()

# Read stock symbols from a text file
def read_symbols(filename):
    with open(filename, 'r') as file:
        symbols = file.read().splitlines()
    return symbols

# Retrieve historical data and calculate min and max percentage difference
def process_symbols(symbols):
    results = {}
    end_date = datetime.now()
    start_date = end_date - timedelta(days=6*30)  # Approximate 6 months
    for symbol in symbols:
        try:
            data = yf.download(symbol, start=start_date, end=end_date)
            min_diff, max_diff = calculate_percentage_difference(data)
            results[symbol] = {'Min Difference': min_diff, 'Max Difference': max_diff}
        except Exception as e:
            results[symbol] = {'Error': str(e)}
    return results

# Main function
def main():
    filename = 'symbols.txt'  # Path to the file containing stock symbols
    symbols = read_symbols(filename)
    results = process_symbols(symbols)
    for symbol, result in results.items():
        if 'Error' in result:
            print(f"{symbol}: Error - {result['Error']}")
        else:
            print(f"{symbol}: Min Difference = {result['Min Difference']:.2f}%, Max Difference = {result['Max Difference']:.2f}%")

if __name__ == "__main__":
    main()