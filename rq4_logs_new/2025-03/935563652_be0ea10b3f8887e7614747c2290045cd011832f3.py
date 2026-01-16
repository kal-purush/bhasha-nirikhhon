import json
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PredictionEvaluator:
    """Evaluates the accuracy of stock predictions by comparing with actual market data"""
    
    def __init__(self, market='UK'):
        self.market = market.upper()
        self.data_dir = Path(__file__).parent / 'data'
        self.market_dir = self.data_dir / self.market.lower()
        self.prediction_history_file = self.market_dir / 'prediction_history.json'
        
    def load_prediction_history(self):
        """Load prediction history from file"""
        try:
            if not self.prediction_history_file.exists():
                logger.error(f"Prediction history file not found: {self.prediction_history_file}")
                return []
            
            with open(self.prediction_history_file, 'r') as f:
                history = json.load(f)
            
            # Ensure the history is a list and has the correct structure
            if not isinstance(history, list):
                logger.warning(f"Invalid prediction history format. Cannot evaluate.")
                return []
                
            return history
            
        except Exception as e:
            logger.error(f"Error loading prediction history: {str(e)}")
            return []
    
    def load_historical_data(self):
        """Load historical market data from chunks"""
        try:
            historical_cache_file = self.market_dir / 'historical_cache.json'
            
            if not historical_cache_file.exists():
                logger.error(f"Historical cache file not found: {historical_cache_file}")
                return {}
            
            with open(historical_cache_file, 'r') as f:
                index = json.load(f)
            
            data_dict = {}
            for chunk_file in index['chunks'].values():
                chunk_path = self.market_dir / chunk_file
                if chunk_path.exists():
                    with open(chunk_path, 'r') as f:
                        chunk_data = json.load(f)
                        for symbol, data in chunk_data.items():
                            try:
                                df = pd.DataFrame(data['data'])
                                if 'Date' in df.columns:
                                    df.set_index('Date', inplace=True)
                                    df.index = pd.to_datetime(df.index)
                                # Ensure Symbol column is present
                                if 'Symbol' not in df.columns:
                                    df['Symbol'] = symbol
                                # Ensure CompanyName column is present
                                if 'CompanyName' not in df.columns and 'company_name' in data:
                                    df['CompanyName'] = data['company_name']
                                elif 'CompanyName' not in df.columns:
                                    df['CompanyName'] = 'Unknown'
                                data_dict[symbol] = df
                            except Exception as e:
                                logger.error(f"Error loading data for {symbol}: {str(e)}")
            
            return data_dict
            
        except Exception as e:
            logger.error(f"Error loading historical data: {str(e)}")
            return {}
    
    def evaluate_predictions(self, evaluation_date=None, days_forward=None):
        """Evaluate prediction accuracy by comparing with actual market data
        
        Args:
            evaluation_date (str, optional): The date to evaluate in format 'YYYY-MM-DD'.
                                           If None, uses the current date.
            days_forward (int, optional): DEPRECATED. This parameter is no longer used as we now
                                         always compare the previous day's prediction with the current day's data.
        
        Returns:
            dict: Evaluation results or None if evaluation fails
        """
        try:
            # Load prediction history
            prediction_history = self.load_prediction_history()
            if not prediction_history:
                logger.error("No prediction history found.")
                return None
            
            # If no evaluation date provided, use the current date
            if evaluation_date is None:
                evaluation_date = datetime.now().strftime('%Y-%m-%d')
                
            # Parse evaluation date
            eval_date = datetime.strptime(evaluation_date, '%Y-%m-%d')
            
            # Sort prediction history by date (newest first)
            prediction_history.sort(key=lambda x: x.get('date', ''), reverse=True)
            
            # Find the most recent prediction before the evaluation date
            prediction_record = None
            for record in prediction_history:
                record_date = datetime.strptime(record['date'], '%Y-%m-%d')
                if record_date < eval_date:  # Find the most recent prediction BEFORE the evaluation date
                    prediction_record = record
                    break
                    
            if not prediction_record:
                logger.error(f"No prediction found before the evaluation date: {evaluation_date}")
                return None
                
            prediction_date = prediction_record['date']
            logger.info(f"Found prediction from {prediction_date} to evaluate against {evaluation_date}")
            
            logger.info(f"Evaluating predictions for date: {prediction_date}")
            
            # Load historical data
            historical_data = self.load_historical_data()
            if not historical_data:
                logger.error("No historical data found.")
                return None
            
            # Parse dates
            pred_date = datetime.strptime(prediction_date, '%Y-%m-%d')
            eval_date_obj = datetime.strptime(evaluation_date, '%Y-%m-%d')
            
            logger.info(f"Looking for actual market data for: {evaluation_date}")
            
            # Check if we have data for the evaluation date in any of the stocks
            has_evaluation_data = False
            for symbol, data in historical_data.items():
                if eval_date_obj in data.index:
                    has_evaluation_data = True
                    break
            
            if not has_evaluation_data:
                logger.error(f"No market data available for evaluation date: {evaluation_date}")
                return None
            
            # Prepare results
            evaluation_results = {
                'prediction_date': prediction_date,
                'evaluation_date': evaluation_date,
                'market': self.market,
                'stocks': [],
                'metrics': {}
            }
            
            # Track metrics for overall evaluation
            actual_returns = []
            predicted_returns = []
            correct_direction = 0
            total_stocks = 0
            
            # Evaluate each stock in the prediction report
            for stock in prediction_record['report']:
                symbol = stock['symbol']
                predicted_return_percent = stock['overview']['predicted_return_percent']
                
                # Skip if we don't have historical data for this stock
                if symbol not in historical_data:
                    logger.warning(f"No historical data found for {symbol}")
                    continue
                
                # Get stock data
                stock_data = historical_data[symbol]
                
                # Use the current price from the prediction as our baseline
                current_price = stock['overview']['current_price']
                
                # Try to get the actual price for the evaluation date
                actual_price = None
                actual_return_percent = None
                has_actual_data = False
                
                # Convert evaluation_date to the same format as the index in stock_data
                try:
                    # Try to find the exact date
                    if eval_date_obj in stock_data.index:
                        # Check if 'Close' is a column or if we need to access it differently
                        if 'Close' in stock_data.columns:
                            actual_price = stock_data.loc[eval_date_obj, 'Close']
                            has_actual_data = True
                        elif 'close' in stock_data.columns:  # Try lowercase version
                            actual_price = stock_data.loc[eval_date_obj, 'close']
                            has_actual_data = True
                        else:  # Try to find any column that might contain closing price
                            price_columns = [col for col in stock_data.columns if 'close' in col.lower() or 'price' in col.lower()]
                            if price_columns:
                                actual_price = stock_data.loc[eval_date_obj, price_columns[0]]
                                has_actual_data = True
                                logger.info(f"Using {price_columns[0]} column for {symbol} closing price")
                    else:
                        logger.warning(f"No exact data found for {symbol} on {evaluation_date}")
                        # We won't try to find closest date as we want to evaluate on the exact date
                except Exception as e:
                    logger.warning(f"Error getting actual price for {symbol} on {evaluation_date}: {str(e)}")
                
                # Calculate actual return if we have the data
                if has_actual_data and actual_price is not None:
                    # Calculate the actual return percentage
                    print(f"DEBUG: {symbol} - actual_price: {actual_price}, current_price: {current_price}")
                    
                    # Calculate actual return based on price difference
                    actual_return_percent = ((actual_price - current_price) / current_price) * 100
                else:
                    logger.warning(f"No actual market data found for {symbol} on {evaluation_date_str}")
                    continue
                
                # Check if prediction direction was correct
                predicted_direction = 1 if predicted_return_percent > 0 else (-1 if predicted_return_percent < 0 else 0)
                actual_direction = 1 if actual_return_percent > 0 else (-1 if actual_return_percent < 0 else 0)
                direction_correct = predicted_direction == actual_direction
                
                if direction_correct:
                    correct_direction += 1
                
                # Add to tracking arrays for overall metrics
                actual_returns.append(actual_return_percent)
                predicted_returns.append(predicted_return_percent)
                total_stocks += 1
                
                # Add stock result to evaluation
                stock_result = {
                    'symbol': symbol,
                    'company_name': stock['company_name'],
                    'predicted_return_percent': predicted_return_percent,
                    'actual_return_percent': round(actual_return_percent, 2),
                    'prediction_error': round(abs(predicted_return_percent - actual_return_percent), 2),
                    'direction_correct': direction_correct
                }
                
                # Add information about the dates being compared
                stock_result['note'] = f"Comparing prediction from {prediction_date} with actual data from {evaluation_date}"
                    
                evaluation_results['stocks'].append(stock_result)
            
            # Calculate overall metrics
            if total_stocks > 0:
                # Convert to numpy arrays for calculations
                actual_returns = np.array(actual_returns)
                predicted_returns = np.array(predicted_returns)
                
                # Mean Absolute Error
                mae = np.mean(np.abs(predicted_returns - actual_returns))
                
                # Root Mean Squared Error
                rmse = np.sqrt(np.mean((predicted_returns - actual_returns) ** 2))
                
                # Direction Accuracy
                direction_accuracy = (correct_direction / total_stocks) * 100
                
                # Add metrics to results
                evaluation_results['metrics'] = {
                    'mean_absolute_error': round(mae, 2),
                    'root_mean_squared_error': round(rmse, 2),
                    'direction_accuracy_percent': round(direction_accuracy, 2),
                    'total_stocks_evaluated': total_stocks
                }
                
                # Add a note about the evaluation method
                evaluation_results['metrics']['note'] = f"Evaluated using prediction from {prediction_date} against actual data from {evaluation_date}"
            else:
                # No stocks could be evaluated
                evaluation_results['metrics'] = {
                    'mean_absolute_error': None,
                    'root_mean_squared_error': None,
                    'direction_accuracy_percent': None,
                    'total_stocks_evaluated': 0,
                    'error_message': 'No stocks could be evaluated. Market data may not be available for the evaluation date.'
                }
            
            # Sort stocks by actual return (best performers first)
            evaluation_results['stocks'].sort(key=lambda x: x['actual_return_percent'], reverse=True)
            
            return evaluation_results
            
        except Exception as e:
            logger.error(f"Error evaluating predictions: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

def main():
    # Create evaluator
    evaluator = PredictionEvaluator(market='UK')
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Evaluate the most recent prediction against today's data
    results = evaluator.evaluate_predictions(evaluation_date=today)
    
    if results:
        print(json.dumps(results, indent=2))
    else:
        print("Evaluation failed. Check logs for details.")
        print("Note: Make sure you have a prediction from a previous day and actual market data for today.")

if __name__ == "__main__":
    main()