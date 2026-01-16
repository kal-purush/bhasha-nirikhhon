import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SentimentProcessor:
    def __init__(self, data_manager=None):
        self.data_manager = data_manager
    
    def add_sentiment_features(self, features_df):
        """
        Add additional sentiment features to the feature dataframe
        - social_sentiment: derived from news_sentiment
        - sector_sentiment: average sentiment for stocks in the same sector
        - market_sentiment: overall market sentiment
        """
        try:
            # Make a copy to avoid modifying the original
            df = features_df.copy()
            
            # Ensure news_sentiment exists
            if 'news_sentiment' not in df.columns:
                logger.warning("news_sentiment column not found, adding with default values")
                df['news_sentiment'] = 0.0
            
            # Add social_sentiment (currently derived from news_sentiment)
            # In a future version, this could be replaced with actual social media sentiment
            df['social_sentiment'] = df['news_sentiment'] * 0.8  # Slightly adjusted version of news sentiment
            
            # Calculate sector_sentiment (simplified version - in reality would group by sector)
            # Group by date and calculate average sentiment
            date_sentiment = df.groupby(df.index.date)['news_sentiment'].mean().reset_index()
            date_sentiment.columns = ['date', 'avg_sentiment']
            
            # Map back to original dataframe
            df['sector_sentiment'] = df.apply(
                lambda row: date_sentiment[
                    date_sentiment['date'] == row.name.date()
                ]['avg_sentiment'].values[0] if len(date_sentiment[
                    date_sentiment['date'] == row.name.date()
                ]) > 0 else 0.0, 
                axis=1
            )
            
            # Calculate market_sentiment (overall average)
            market_avg_sentiment = df['news_sentiment'].mean()
            df['market_sentiment'] = market_avg_sentiment
            
            logger.info("Added sentiment features: social_sentiment, sector_sentiment, market_sentiment")
            return df
            
        except Exception as e:
            logger.error(f"Error adding sentiment features: {str(e)}")
            # Return original dataframe if there's an error
            return features_df
    
    def get_sentiment_summary(self, symbol, features_df):
        """
        Get a summary of sentiment features for a specific symbol
        """
        try:
            # Filter for the symbol
            symbol_data = features_df[features_df['Symbol'] == symbol]
            
            if symbol_data.empty:
                return {
                    'news_sentiment': 0.0,
                    'social_sentiment': 0.0,
                    'sector_sentiment': 0.0,
                    'market_sentiment': 0.0
                }
            
            # Get the latest data
            latest = symbol_data.iloc[-1]
            
            # Create summary
            summary = {
                'news_sentiment': float(latest.get('news_sentiment', 0.0)),
                'social_sentiment': float(latest.get('social_sentiment', 0.0)),
                'sector_sentiment': float(latest.get('sector_sentiment', 0.0)),
                'market_sentiment': float(latest.get('market_sentiment', 0.0))
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting sentiment summary for {symbol}: {str(e)}")
            return {
                'news_sentiment': 0.0,
                'social_sentiment': 0.0,
                'sector_sentiment': 0.0,
                'market_sentiment': 0.0
            }