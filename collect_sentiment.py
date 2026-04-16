import os
import csv
import time
from datetime import datetime
import pandas as pd
from textblob import TextBlob
from yahoo_fin import news
import praw
import feedparser
import requests

OUTPUT_FILE = "sentiment_with_pct_change.csv"
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")

# ===== Load S&P 500 tickers =====
sp500_table = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
tickers = sp500_table["Symbol"].str.replace(".", "-", regex=False).tolist()

# ===== Load historical close prices for prev_close and pct_change =====
hist_df = pd.read_csv("sp500_cleaned_data.csv", parse_dates=["Date"])
hist_df["Prev_Close"] = hist_df.groupby("Ticker")["Close"].shift(1)
hist_df["Pct_Change"] = ((hist_df["Close"] - hist_df["Prev_Close"]) / hist_df["Prev_Close"]) * 100

# ===== Reddit API =====
def get_reddit_posts(symbol):
    reddit = praw.Reddit(
        client_id="Lym7imwjxC3vDEnfildUrA",
        client_secret="QD9fuT-tuJNpMAAz4ti_uYl5zVRibA",
        user_agent="sentiment-script"
    )
    posts = []
    for subreddit in ["stocks", "investing"]:
        for post in reddit.subreddit(subreddit).search(symbol, limit=10):
            posts.append(post.title)
    return posts

# ===== Yahoo Finance =====
def get_yahoo_news(symbol):
    try:
        return [item["title"] for item in news.get_yf_rss(symbol)]
    except:
        return []

# ===== Google News =====
def get_google_news(query):
    url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    feed = feedparser.parse(url)
    return [entry.title for entry in feed.entries]

# ===== NewsAPI =====
def get_newsapi_news(query):
    if not NEWSAPI_KEY:
        return []
    url = f"https://newsapi.org/v2/everything?q={query}&language=en&pageSize=5&apiKey={NEWSAPI_KEY}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return [a["title"] for a in data.get("articles", [])]
    except:
        return []
    return []

# ===== Sentiment Analysis =====
def analyze_sentiment(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0:
        return "positive"
    elif polarity < 0:
        return "negative"
    else:
        return "neutral"

# ===== Main =====
final_rows = []
today = datetime.utcnow().date().isoformat()

for ticker in tickers:
    positive, negative, neutral = 0, 0, 0
    all_texts = []

    # Collect news & posts
    sources = [
        get_reddit_posts(ticker),
        get_yahoo_news(ticker),
        get_google_news(ticker),
        get_newsapi_news(ticker)
    ]
    for src in sources:
        all_texts.extend(src)

    # Analyze sentiment
    for text in all_texts:
        s = analyze_sentiment(text)
        if s == "positive": positive += 1
        elif s == "negative": negative += 1
        else: neutral += 1

    total = positive + negative + neutral
    pos_percent = round(positive / total, 4) if total > 0 else 0

    final_rows.append([
        today, ticker, positive, negative, neutral, total,
        pos_percent
    ])

    print(f"[+] {ticker}: {total} mentions | Pos%: {pos_percent}")
    time.sleep(1)  # avoid bans

# ===== Save CSV (append mode with duplicate check) =====
file_exists = os.path.isfile(OUTPUT_FILE)

with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    # Write header if file doesn't exist
    if not file_exists:
        writer.writerow([
            "Date", "Ticker", "Positive_Count", "Negative_Count", "Neutral_Count",
            "Total_Mentions", "Positive_Percent"
        ])

    if file_exists:
        existing_df = pd.read_csv(OUTPUT_FILE)
        existing_set = set(zip(existing_df["Date"], existing_df["Ticker"]))
        new_rows = [row for row in final_rows if (row[0], row[1]) not in existing_set]
        writer.writerows(new_rows)
        print(f"[+] Appended {len(new_rows)} new rows to {OUTPUT_FILE}")
    else:
        writer.writerows(final_rows)
        print(f"[+] Saved {len(final_rows)} tickers to {OUTPUT_FILE}")
