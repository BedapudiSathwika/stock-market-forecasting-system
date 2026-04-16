import pandas as pd
import numpy as np
import boto3
import os

# Toggle: Use unadjusted Close (True) or adjusted Close (False)
USE_UNADJUSTED_CLOSE = True

# Step 1: Load and validate historical data
try:
    df = pd.read_csv("sp500_historical_data.csv", parse_dates=["Date"])
    print("Loaded sp500_historical_data.csv successfully.")
except FileNotFoundError:
    print("Error: sp500_historical_data.csv not found.")
    exit(1)

expected_cols = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
if not all(col in df.columns for col in expected_cols):
    print(f"Error: Missing expected columns.")
    exit(1)

# Step 2: Close price handling
if USE_UNADJUSTED_CLOSE and "Adj Close" in df.columns:
    df["Close"] = df["Close"]
elif not USE_UNADJUSTED_CLOSE and "Adj Close" in df.columns:
    df["Close"] = df["Adj Close"]

# Step 3: Clean date/time
df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)

# Step 4: Handle missing values
numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
df[numeric_cols] = df[numeric_cols].ffill()
df = df.dropna(subset=["Ticker", "Close"])

# Step 5: Remove duplicates
df = df.drop_duplicates(subset=["Date", "Ticker"])

# Step 6: Remove extreme outliers
if len(df) > 50:
    df["Price_Median"] = df.groupby("Ticker")["Close"].transform("median")
    df = df[df["Close"] <= 10 * df["Price_Median"]]
    df = df.drop(columns=["Price_Median"])

# Step 7: Standardize date format
df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

# Step 8: Add sector info
try:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500_table = pd.read_html(url)[0][["Symbol", "GICS Sector"]]
    sp500_table.columns = ["Ticker", "Sector"]
    sp500_table["Ticker"] = sp500_table["Ticker"].str.replace("BRK.B", "BRK-B").str.replace(".", "-", regex=False)
    df = df.merge(sp500_table, on="Ticker", how="left")
    df["Sector"] = df["Sector"].fillna("Unknown")
except Exception as e:
    print(f"Error scraping Wikipedia: {e}")
    df["Sector"] = "Unknown"

# Step 9: Add MA50
df["MA50"] = df.groupby("Ticker")["Close"].transform(lambda x: x.rolling(window=50, min_periods=1).mean())

# Step 10: Add Prev_Close & Pct_Change
df = df.sort_values(["Ticker", "Date"])
df["Prev_Close"] = df.groupby("Ticker")["Close"].shift(1)
df["Pct_Change"] = ((df["Close"] - df["Prev_Close"]) / df["Prev_Close"]) * 100

# Step 11: Select final columns
df = df[[
    "Date", "Ticker", "Open", "High", "Low", "Close", "Volume",
    "Sector", "MA50", "Prev_Close", "Pct_Change"
]]

# Step 12: Save locally
df.to_csv("sp500_cleaned_data.csv", index=False)
df.to_parquet("sp500_cleaned_data.parquet")

print(f"Saved cleaned file with {len(df)} rows and {df['Ticker'].nunique()} tickers.")
