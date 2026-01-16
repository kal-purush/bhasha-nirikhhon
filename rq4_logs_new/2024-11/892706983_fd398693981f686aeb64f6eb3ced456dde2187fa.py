import pandas as pd
import random
import re
from yahoo_fin import stock_info as si

# Load the data with combined scores
output_file_path = r'C:\Users\Andrew\Desktop\NEU\5100 foundation for AI\Final\Reddit WallStreetBets Posts Sentiment Analysis\reddit_wsb_combined_scores.csv'
classified_file_path = r'C:\Users\Andrew\Desktop\NEU\5100 foundation for AI\Final\Reddit WallStreetBets Posts Sentiment Analysis\reddit_wsb_classified.csv'
reddit_data_with_scores = pd.read_csv(output_file_path)

# Function to classify the sentiment based on the combined score
def classify_sentiment(combined_score):
    if combined_score > 0.002:
        return 'Positive'
    elif combined_score < 0.001:
        return 'Negative'
    else:
        return 'Neutral'

# Apply the classification to the combined score
reddit_data_with_scores['sentiment_classification'] = reddit_data_with_scores['combined_score'].apply(classify_sentiment)

# Get a list of all tickers from Yahoo Finance
tickers_list = si.tickers_sp500() + si.tickers_nasdaq() + si.tickers_dow()  # Combine multiple sources as needed
tickers_set = set(tickers_list)

# Function to identify tickers in the text and sort them alphabetically
def identify_tickers(text):
    words = re.findall(r'\b[A-Z]{1,5}\b', text)  # Match words that are 1-5 capital letters long
    tickers = sorted([word for word in words if word in tickers_set])
    return tickers

# Apply the identify_tickers function to each post
reddit_data_with_scores['tickers'] = reddit_data_with_scores['body'].apply(identify_tickers)

# Function to clean text by removing excess whitespace
def clean_text(text):
    text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces/newlines with a single space
    return text.strip()

# Apply text cleaning to the body
reddit_data_with_scores['body'] = reddit_data_with_scores['body'].apply(clean_text)

# Sample 5 posts from each sentiment category for inspection
def sample_posts_by_sentiment(df, sentiment, n=5):
    filtered_df = df[df['sentiment_classification'] == sentiment]
    if len(filtered_df) >= n:
        return filtered_df.sample(n=n, random_state=random.randint(1, 10000))
    else:
        return filtered_df

positive_posts = sample_posts_by_sentiment(reddit_data_with_scores, 'Positive', 5)
neutral_posts = sample_posts_by_sentiment(reddit_data_with_scores, 'Neutral', 5)
negative_posts = sample_posts_by_sentiment(reddit_data_with_scores, 'Negative', 5)

sampled_posts = pd.concat([positive_posts, neutral_posts, negative_posts])

# Debugging: Display sampled posts and ticker sentiment summary
print("Sampled Posts with Sentiment Classification:")
print(sampled_posts[['body', 'tickers', 'score', 'comms_num', 'transformer_sentiment_score', 'combined_score', 'sentiment_classification']])

ticker_sentiment_summary = sampled_posts.explode('tickers').groupby(['tickers', 'sentiment_classification']).size().unstack(fill_value=0)
print("Ticker sentiment summary:")
print(ticker_sentiment_summary)

# Save the DataFrame with sentiment classification to a new CSV file
reddit_data_with_scores.to_csv(classified_file_path, index=False)
print(f"Data with sentiment classification saved to {classified_file_path}")