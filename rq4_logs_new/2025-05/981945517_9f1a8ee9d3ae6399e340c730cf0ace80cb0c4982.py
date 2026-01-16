import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from dataclasses import dataclass
import logging
import json
import matplotlib.pyplot as plt
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_market_data(filepath: str) -> Dict[str, Dict[int, Tuple[float, int]]]:
    """
    Load and process L1 data into snapshots.
    
    Args:
        filepath: Path to the CSV file containing market data
        
    Returns:
        Dictionary of snapshots where each snapshot contains venue data
    """
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Successfully loaded data from {filepath}")
        logger.info(f"Total rows in CSV: {len(df)}")
        logger.info(f"Columns in CSV: {df.columns.tolist()}")
        logger.info(f"Unique publishers: {df['publisher_id'].nunique()}")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise
    
    # Sort and keep first record per publisher per timestamp
    df = df.sort_values(['ts_event', 'publisher_id'])
    df_first = df.groupby(['ts_event', 'publisher_id']).first().reset_index()
    
    logger.info(f"Rows after keeping first record per publisher: {len(df_first)}")
    logger.info(f"Unique timestamps: {df_first['ts_event'].nunique()}")
    
    # Create snapshots dictionary
    snapshots = {}
    for ts in df_first['ts_event'].unique():
        ts_data = df_first[df_first['ts_event'] == ts]
        ts_dict = dict(zip(ts_data['publisher_id'], 
                          zip(ts_data['ask_px_00'], ts_data['ask_sz_00'])))
        snapshots[ts] = ts_dict
    
    logger.info(f"Processed {len(snapshots)} snapshots")
    
    # Log venue statistics
    venue_counts = [len(venues) for venues in snapshots.values()]
    logger.info(f"Average venues per snapshot: {np.mean(venue_counts):.2f}")
    logger.info(f"Max venues in a snapshot: {max(venue_counts)}")
    logger.info(f"Min venues in a snapshot: {min(venue_counts)}")
    
    return snapshots

# Part 2: Static Allocator
@dataclass
class Venue:
    """
    Represents a trading venue with its pricing and capacity information.
    
    Attributes:
        ask: Ask price at the venue
        ask_size: Available size at the ask price
        fee: Trading fee at the venue
        rebate: Maker rebate at the venue
    """
    ask: float
    ask_size: int
    fee: float = 0.0
    rebate: float = 0.0

def allocate(order_size: int, 
            venues: List[Venue], 
            lambda_over: float, 
            lambda_under: float, 
            theta_queue: float) -> Tuple[List[int], float]:
    """
    Implements the Cont-Kukanov allocator algorithm.
    
    Args:
        order_size: Target number of shares to buy
        venues: List of Venue objects with ask prices and sizes
        lambda_over: Cost penalty per extra share bought
        lambda_under: Cost penalty per unfilled share
        theta_queue: Queue-risk penalty (linear in total mis-execution)
    
    Returns:
        Tuple of (best_split, best_cost) where:
        - best_split: List of shares to allocate to each venue
        - best_cost: Total expected cost of the allocation
    """
    if not venues:
        raise ValueError("No venues provided")
    if order_size <= 0:
        raise ValueError("Order size must be positive")
    
    # Validate venue sizes
    total_available = sum(v.ask_size for v in venues)
    if total_available < order_size:
        logger.warning(f"Insufficient venue sizes: {total_available} < {order_size}")
        return [], float('inf')
    
    # Log allocation attempt
    logger.info(f"Attempting allocation for order_size={order_size}")
    logger.info(f"Venue sizes: {[v.ask_size for v in venues]}")
    
    step = 100  # As per pseudocode
    splits = [[]]
    
    # Generate all possible splits
    for v in range(len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v].ask_size)
            for q in range(0, max_v + 1, step):
                new_splits.append(alloc + [q])
        splits = new_splits
    
    # Find best split
    best_cost = float('inf')
    best_split = []
    
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, 
                          lambda_over, lambda_under, theta_queue)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc
    
    if not best_split:
        logger.warning("No valid allocation found")
        return [], float('inf')
    
    return best_split, best_cost

def compute_cost(split: List[int], 
                venues: List[Venue], 
                order_size: int,
                lambda_over: float, 
                lambda_under: float, 
                theta_queue: float) -> float:
    """
    Computes the total cost for a given allocation.
    
    Args:
        split: List of shares allocated to each venue
        venues: List of Venue objects
        order_size: Target order size
        lambda_over: Cost penalty per extra share
        lambda_under: Cost penalty per unfilled share
        theta_queue: Queue-risk penalty
    
    Returns:
        Total cost including execution cost, penalties, and rebates
    """
    if len(split) != len(venues):
        raise ValueError("Split length must match number of venues")
    
    executed = 0
    cash_spent = 0
    
    for i, venue in enumerate(venues):
        exe = min(split[i], venue.ask_size)
        executed += exe
        cash_spent += exe * (venue.ask + venue.fee)
        maker_rebate = max(split[i] - exe, 0) * venue.rebate
        cash_spent -= maker_rebate
    
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk_pen = theta_queue * (underfill + overfill)
    cost_pen = lambda_under * underfill + lambda_over * overfill
    
    return cash_spent + risk_pen + cost_pen

def search_parameters(snapshots: Dict[str, Dict[int, Tuple[float, int]]]) -> Dict:
    """
    Search for optimal parameters using random search
    
    Args:
        snapshots: Dictionary of market snapshots
        
    Returns:
        Dictionary containing best parameters and their results
    """
    best_cost = float('inf')
    best_params = None
    best_results = None
    
    # Parameter ranges based on Cont & Kukanov paper
    param_ranges = {
        'lambda_over': (0.1, 2.0),   # Penalty for overfilling
        'lambda_under': (0.1, 2.0),  # Penalty for underfilling
        'theta_queue': (0.1, 1.0)    # Queue risk penalty
    }
    
    n_combinations = 100
    
    for i in range(n_combinations):
        params = {
            'lambda_over': np.random.uniform(*param_ranges['lambda_over']),
            'lambda_under': np.random.uniform(*param_ranges['lambda_under']),
            'theta_queue': np.random.uniform(*param_ranges['theta_queue'])
        }
        
        results = simulate_execution(snapshots, **params)
        
        # Only consider complete fills
        if results['total_filled'] == 5000:
            if results['total_cost'] < best_cost:
                best_cost = results['total_cost']
                best_params = params
                best_results = results
                logger.info(f"New best parameters found: {best_params}")
                logger.info(f"Cost: {best_cost}")
        
        if (i + 1) % 10 == 0:
            logger.info(f"Tested {i + 1}/{n_combinations} combinations")
    
    if best_params is None:
        logger.warning("No valid parameter combination found that fills the entire order")
        # Fall back to default parameters
        best_params = {
            'lambda_over': 0.1,
            'lambda_under': 0.1,
            'theta_queue': 0.1
        }
        best_results = simulate_execution(snapshots, **best_params)
    
    return {
        'best_parameters': best_params,
        'results': best_results
    }

# Part 3: Execution Simulation
def simulate_execution(snapshots: Dict[str, Dict[int, Tuple[float, int]]],
                      target_size: int = 5000,
                      lambda_over: float = 1.0,
                      lambda_under: float = 1.0,
                      theta_queue: float = 0.1) -> Dict:
    """
    Simulate execution using the allocator.
    
    Args:
        snapshots: Dictionary of market snapshots
        target_size: Target order size
        lambda_over: Cost penalty per extra share
        lambda_under: Cost penalty per unfilled share
        theta_queue: Queue-risk penalty
    
    Returns:
        Dictionary containing execution results
    """
    total_filled = 0
    total_cost = 0
    unfilled = target_size
    execution_history = []
    
    for ts, venue_data in snapshots.items():
        if total_filled >= target_size:
            break
            
        # Convert venue data to Venue objects
        venues = [Venue(ask=price, ask_size=size) 
                 for price, size in venue_data.values()]
        
        # Get allocation
        allocation, _ = allocate(unfilled, venues, 
                              lambda_over, lambda_under, theta_queue)
        
        # Execute orders
        for venue, alloc in zip(venues, allocation):
            fill = min(alloc, venue.ask_size)
            if fill > 0:
                total_filled += fill
                total_cost += fill * venue.ask
                unfilled -= fill
                
                execution_history.append({
                    'timestamp': ts,
                    'shares_filled': fill,
                    'price': venue.ask,
                    'cumulative_cost': total_cost
                })
                
        logger.info(f"Progress: {total_filled}/{target_size} shares filled")
    
    logger.info(f"Execution completed: {total_filled}/{target_size} shares filled")
    return {
        'total_filled': total_filled,
        'total_cost': total_cost,
        'avg_price': total_cost / total_filled if total_filled > 0 else 0,
        'execution_history': execution_history
    }

# Part 4: Baseline Strategies
def best_ask_strategy(snapshots: Dict[str, Dict[int, Tuple[float, int]]],
                     target_size: int = 5000) -> Dict:
    """
    Always hit the lowest ask at each timestamp.
    
    Args:
        snapshots: Dictionary of market snapshots
        target_size: Target order size
    
    Returns:
        Dictionary containing execution results
    """
    total_filled = 0
    total_cost = 0
    execution_history = []
    
    for ts, venue_data in snapshots.items():
        if total_filled >= target_size:
            break
            
        # Find best ask
        best_venue = min(venue_data.items(), 
                        key=lambda x: x[1][0])  # x[1][0] is ask price
        price, size = best_venue[1]
        fill = min(size, target_size - total_filled)
        
        if fill > 0:
            total_filled += fill
            total_cost += fill * price
            execution_history.append({
                'timestamp': ts,
                'shares_filled': fill,
                'price': price,
                'cumulative_cost': total_cost
            })
    
    return {
        'total_filled': total_filled,
        'total_cost': total_cost,
        'avg_price': total_cost / total_filled if total_filled > 0 else 0,
        'execution_history': execution_history
    }

def twap_strategy(snapshots: Dict[str, Dict[int, Tuple[float, int]]],
                 target_size: int = 5000,
                 bucket_seconds: int = 60) -> Dict:
    """
    Uniformly split orders over time buckets.
    
    Args:
        snapshots: Dictionary of market snapshots
        target_size: Target order size
        bucket_seconds: Size of time buckets in seconds
    
    Returns:
        Dictionary containing execution results
    """
    timestamps = sorted(snapshots.keys())
    if not timestamps:
        return {'total_filled': 0, 'total_cost': 0, 'avg_price': 0}
    
    start_time = pd.to_datetime(timestamps[0])
    end_time = pd.to_datetime(timestamps[-1])
    total_seconds = (end_time - start_time).total_seconds()
    num_buckets = max(1, int(total_seconds / bucket_seconds))
    bucket_size = target_size / num_buckets
    
    total_filled = 0
    total_cost = 0
    execution_history = []
    current_bucket = -1
    bucket_filled = 0
    
    for ts_str in timestamps:
        if total_filled >= target_size:
            break
            
        current_time = pd.to_datetime(ts_str)
        seconds_from_start = (current_time - start_time).total_seconds()
        bucket = int(seconds_from_start / bucket_seconds)
        
        if bucket > current_bucket:
            current_bucket = bucket
            bucket_filled = 0
            
        remaining = bucket_size - bucket_filled
        if remaining <= 0:
            continue
            
        # Execute at best ask
        best_venue = min(snapshots[ts_str].items(), 
                        key=lambda x: x[1][0])  # x[1][0] is ask price
        price, size = best_venue[1]
        fill = min(size, remaining, target_size - total_filled)
        
        if fill > 0:
            total_filled += fill
            total_cost += fill * price
            bucket_filled += fill
            execution_history.append({
                'timestamp': ts_str,
                'shares_filled': fill,
                'price': price,
                'cumulative_cost': total_cost
            })
    
    return {
        'total_filled': total_filled,
        'total_cost': total_cost,
        'avg_price': total_cost / total_filled if total_filled > 0 else 0,
        'execution_history': execution_history
    }

def vwap_strategy(snapshots: Dict[str, Dict[int, Tuple[float, int]]],
                 target_size: int = 5000) -> Dict:
    """
    Weight prices by displayed ask size.
    
    Args:
        snapshots: Dictionary of market snapshots
        target_size: Target order size
    
    Returns:
        Dictionary containing execution results
    """
    total_filled = 0
    total_cost = 0
    execution_history = []
    
    for ts, venue_data in snapshots.items():
        if total_filled >= target_size:
            break
            
        # Calculate VWAP for this snapshot
        snapshot_volume = sum(size for _, size in venue_data.values())
        if snapshot_volume == 0:
            continue
            
        snapshot_vwap = sum(price * size for price, size in venue_data.values()) / snapshot_volume
        
        # Execute at VWAP
        best_venue = min(venue_data.items(), key=lambda x: x[1][0])
        price, size = best_venue[1]
        fill = min(size, target_size - total_filled)
        
        if fill > 0:
            total_filled += fill
            total_cost += fill * snapshot_vwap
            execution_history.append({
                'timestamp': ts,
                'shares_filled': fill,
                'price': snapshot_vwap,
                'cumulative_cost': total_cost
            })
    
    return {
        'total_filled': total_filled,
        'total_cost': total_cost,
        'avg_price': total_cost / total_filled if total_filled > 0 else 0,
        'execution_history': execution_history
    }

def calculate_bps_savings(cont_kukanov_results: Dict, baseline_results: Dict) -> float:
    """
    Calculate savings in basis points
    """
    if baseline_results['total_cost'] == 0:
        return 0.0
    return ((baseline_results['total_cost'] - cont_kukanov_results['total_cost']) / 
            baseline_results['total_cost'] * 10000)

def format_results(cont_kukanov_results: Dict, 
                  best_ask_results: Dict,
                  twap_results: Dict,
                  vwap_results: Dict) -> str:
    """
    Format results as required JSON
    """
    results = {
        'best_parameters': cont_kukanov_results['best_parameters'],
        'cont_kukanov': {
            'total_cost': cont_kukanov_results['results']['total_cost'],
            'avg_price': cont_kukanov_results['results']['avg_price']
        },
        'best_ask': {
            'total_cost': best_ask_results['total_cost'],
            'avg_price': best_ask_results['avg_price']
        },
        'twap': {
            'total_cost': twap_results['total_cost'],
            'avg_price': twap_results['avg_price']
        },
        'vwap': {
            'total_cost': vwap_results['total_cost'],
            'avg_price': vwap_results['avg_price']
        },
        'savings_bps': {
            'best_ask': calculate_bps_savings(cont_kukanov_results['results'], best_ask_results),
            'twap': calculate_bps_savings(cont_kukanov_results['results'], twap_results),
            'vwap': calculate_bps_savings(cont_kukanov_results['results'], vwap_results)
        }
    }
    return json.dumps(results, indent=2)

def plot_cumulative_costs(cont_kukanov_results: Dict,
                         best_ask_results: Dict,
                         twap_results: Dict,
                         vwap_results: Dict) -> None:
    """
    Generate cumulative cost plot
    """
    plt.figure(figsize=(10, 6))
    
    # Plot cumulative costs
    plt.plot([0, 5000], [0, cont_kukanov_results['results']['total_cost']], 
             label='Cont-Kukanov', linewidth=2)
    plt.plot([0, 5000], [0, best_ask_results['total_cost']], 
             label='Best Ask', linestyle='--')
    plt.plot([0, 5000], [0, twap_results['total_cost']], 
             label='TWAP', linestyle=':')
    plt.plot([0, 5000], [0, vwap_results['total_cost']], 
             label='VWAP', linestyle='-.')
    
    # Add labels and title
    plt.xlabel('Shares Executed')
    plt.ylabel('Cumulative Cost ($)')
    plt.title('Cumulative Cost Comparison')
    plt.legend()
    plt.grid(True)
    
    # Save plot
    plt.savefig('results.png')
    plt.close()

def main():
    try:
        # Load and process market data
        snapshots = load_market_data('cont&Kukanov/l1_day.csv')
        
        # Run parameter search for Cont-Kukanov strategy
        cont_kukanov_results = search_parameters(snapshots)
        
        # Run baseline strategies
        best_ask_results = best_ask_strategy(snapshots)
        twap_results = twap_strategy(snapshots)
        vwap_results = vwap_strategy(snapshots)
        
        # Generate plot
        plot_cumulative_costs(cont_kukanov_results,
                            best_ask_results,
                            twap_results,
                            vwap_results)
        
        # Format and print results
        results_json = format_results(cont_kukanov_results,
                                    best_ask_results,
                                    twap_results,
                                    vwap_results)
        print(results_json)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()