
import yfinance as yf
import pandas as pd
from datetime import datetime

# Load tickers
try:
    tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
    tickers = [t.replace("BRK.B", "BRK-B").replace(".", "-") for t in tickers]
    print(f"Loaded {len(tickers)} tickers.")
except Exception as e:
    print(f"Error loading tickers from Wikipedia: {e}")
    exit(1)

# Download data
start_date = "2020-01-01"
end_date = datetime.today().strftime("%Y-%m-%d")
data = []
failed_tickers = []

for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date, interval="1d", auto_adjust=False)
        if not df.empty:
            df["Ticker"] = ticker
            data.append(df)
            print(f"Downloaded {ticker}")
        else:
            print(f"No data for {ticker}")
            failed_tickers.append(ticker)
    except Exception as e:
        print(f"Error downloading {ticker}: {e}")
        failed_tickers.append(ticker)

# Log failed tickers
if failed_tickers:
    print(f"Failed tickers: {failed_tickers}")

# Concatenate non-empty DataFrames
if data:
    all_data = pd.concat(data)
    all_data.reset_index().to_csv("sp500_historical_data.csv", index=False)
    print("Saved sp500_historical_data.csv")
    print(f"Total rows: {len(all_data)}")
    print(f"Unique tickers: {all_data['Ticker'].nunique()}")
    print(f"Columns: {all_data.columns.tolist()}")
else:
    print("Error: No data downloaded.")
    exit(1)