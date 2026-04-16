import pandas as pd

# Scrape S&P 500 tickers from Wikipedia
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
tables = pd.read_html(url)
sp500_table = tables[0]
tickers = sp500_table["Symbol"].tolist()

# Save tickers to a CSV file
pd.DataFrame(tickers, columns=["Ticker"]).to_csv("sp500_tickers.csv", index=False)
print(f"Collected {len(tickers)} tickers.")
