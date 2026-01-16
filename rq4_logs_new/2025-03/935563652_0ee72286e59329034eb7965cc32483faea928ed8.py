import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
from collections import deque

logger = logging.getLogger(__name__)

class PredictionHistoryManager:
    """Manages the history of stock predictions, keeping track of the last 5 prediction records"""
    
    def __init__(self, market='UK'):
        self.market = market.upper()
        self.data_dir = Path(__file__).parent.parent / 'data'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Ensure market-specific directories exist
        self.market_dir = self.data_dir / self.market.lower()
        self.market_dir.mkdir(parents=True, exist_ok=True)
        
        # File to store prediction history
        self.prediction_history_file = self.market_dir / 'prediction_history.json'
        
        # Maximum number of prediction records to keep
        self.max_records = 5
        
        # Initialize prediction history
        self.prediction_history = self._load_prediction_history()
    
    def _load_prediction_history(self):
        """Load prediction history from file"""
        try:
            if not self.prediction_history_file.exists():
                return []
            
            with open(self.prediction_history_file, 'r') as f:
                history = json.load(f)
            
            # Ensure the history is a list and has the correct structure
            if not isinstance(history, list):
                logger.warning(f"Invalid prediction history format. Resetting history.")
                return []
            
            # Sort by date (newest first)
            history.sort(key=lambda x: x.get('date', ''), reverse=True)
            
            # Limit to max_records
            return history[:self.max_records]
            
        except Exception as e:
            logger.error(f"Error loading prediction history: {str(e)}")
            return []
    
    def save_prediction(self, predictions, report, analysis_date=None):
        """Save a new prediction record to history"""
        try:
            # Use current date if not provided
            if analysis_date is None:
                analysis_date = datetime.now()
            
            # Format date as string
            date_str = analysis_date.strftime('%Y-%m-%d')
            
            # Create prediction record
            prediction_record = {
                'date': date_str,
                'timestamp': datetime.now().isoformat(),
                'market': self.market,
                'report': report
            }
            
            # Add to history (at the beginning)
            self.prediction_history.insert(0, prediction_record)
            
            # Limit to max_records
            if len(self.prediction_history) > self.max_records:
                self.prediction_history = self.prediction_history[:self.max_records]
            
            # Save to file
            with open(self.prediction_history_file, 'w') as f:
                json.dump(self.prediction_history, f, indent=2)
            
            logger.info(f"Saved prediction for {date_str} to history")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving prediction to history: {str(e)}")
            return False
    
    def get_prediction_history(self):
        """Get the prediction history"""
        return self.prediction_history
    
    def get_prediction_by_date(self, date_str):
        """Get a specific prediction by date"""
        try:
            for record in self.prediction_history:
                if record['date'] == date_str:
                    return record
            return None
        except Exception as e:
            logger.error(f"Error getting prediction by date: {str(e)}")
            return None
    
    def clear_history(self):
        """Clear the prediction history"""
        try:
            self.prediction_history = []
            with open(self.prediction_history_file, 'w') as f:
                json.dump(self.prediction_history, f)
            logger.info("Cleared prediction history")
            return True
        except Exception as e:
            logger.error(f"Error clearing prediction history: {str(e)}")
            return False
    
    def format_prediction_for_display(self, prediction_record):
        """Format a prediction record for display"""
        try:
            if not prediction_record:
                return None
            
            formatted = {
                'date': prediction_record['date'],
                'market': prediction_record['market'],
                'stocks': []
            }
            
            # Format each stock in the report
            for stock in prediction_record['report']:
                formatted_stock = {
                    'symbol': stock['symbol'],
                    'company_name': stock['company_name'],
                    'current_price': stock['overview']['current_price'],
                    'predicted_price': stock['overview']['predicted_price'],
                    'predicted_return_percent': stock['overview']['predicted_return_percent'],
                    'news_sentiment': stock['market_sentiment']['news_sentiment_score']
                }
                formatted['stocks'].append(formatted_stock)
            
            return formatted
            
        except Exception as e:
            logger.error(f"Error formatting prediction for display: {str(e)}")
            return None