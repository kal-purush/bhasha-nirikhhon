from datetime import datetime, timedelta
import time
import logging
import requests
import json
from pathlib import Path
from typing import Callable, Any, Dict, Optional, Union

# Rate limiting libraries
from pyrate_limiter import Duration, RequestRate, Limiter
from requests_ratelimiter import LimiterSession
import requests_cache

logger = logging.getLogger(__name__)

from queue import Queue, Empty
from threading import Thread, Event
from typing import Dict, List, Tuple

class EnhancedAPIRateLimiter:
    """Enhanced API Rate Limiter with improved caching, adaptive rate limiting, and request queuing"""
    
    def __init__(self, cache_dir: Optional[Path] = None, max_queue_size: int = 1000):
        # Set up cache directory
        if cache_dir is None:
            cache_dir = Path(__file__).parent.parent / 'cache'
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize cached sessions with different expiration times
        self.setup_cached_sessions()
        
        # YFinance rate limits (2000 requests per hour per IP)
        self.yf_limiter = Limiter(
            RequestRate(20, Duration.MINUTE),   # 20 requests per minute for safety
            RequestRate(1500, Duration.HOUR)  # 1500 requests per hour (buffer for safety)
        )
        
        # NewsAPI rate limits (100 requests per day for free tier)
        self.news_limiter = Limiter(
            RequestRate(4, Duration.MINUTE),    # 4 requests per minute for safety
            RequestRate(90, Duration.DAY)     # 90 requests per day (buffer for safety)
        )
        
        # Wikipedia rate limits (conservative limits to be polite)
        self.wiki_limiter = Limiter(
            RequestRate(8, Duration.MINUTE),    # 8 requests per minute
            RequestRate(500, Duration.HOUR)   # 500 requests per hour (increased from 180)
        )
        
        # Alpha Vantage rate limits (500 requests per day, 5 per minute for free tier)
        self.alpha_vantage_limiter = Limiter(
            RequestRate(4, Duration.MINUTE),    # 4 requests per minute for safety
            RequestRate(450, Duration.DAY)    # 450 requests per day (buffer for safety)
        )
        
        # Initialize last request timestamps
        self.last_yf_request = datetime.min
        self.last_news_request = datetime.min
        self.last_wiki_request = datetime.min
        self.last_alpha_vantage_request = datetime.min
        
        # Backoff settings
        self.max_retries = 5  # Increased from 3
        self.base_backoff = 2  # Base backoff in seconds
        
        # Rate limit tracking
        self.rate_limit_hits = {
            'yfinance': 0,
            'newsapi': 0,
            'wikipedia': 0,
            'alphavantage': 0
        }
        self.last_rate_limit_reset = datetime.now()
        
        # Request queues for each service
        self.request_queues = {
            'yfinance': Queue(maxsize=max_queue_size),
            'newsapi': Queue(maxsize=max_queue_size),
            'wikipedia': Queue(maxsize=max_queue_size),
            'alphavantage': Queue(maxsize=max_queue_size)
        }
        
        # Queue processor threads
        self.queue_processors = {}
        self.stop_events = {}
        
        # Start queue processors
        for service in self.request_queues:
            self.stop_events[service] = Event()
            self.queue_processors[service] = Thread(
                target=self._process_queue,
                args=(service,),
                daemon=True
            )
            self.queue_processors[service].start()
        
        # Load cached rate limit state if exists
        self._load_rate_limit_state()
    
    def setup_cached_sessions(self):
        """Set up cached sessions with different expiration times"""
        # YFinance cache - expire after 6 hours for price data
        self.yf_session = requests_cache.CachedSession(
            str(self.cache_dir / 'yfinance_cache'),
            backend='sqlite',
            expire_after=timedelta(hours=6),
            allowable_methods=('GET', 'POST')
        )
        
        # News cache - expire after 12 hours
        self.news_session = requests_cache.CachedSession(
            str(self.cache_dir / 'news_cache'),
            backend='sqlite',
            expire_after=timedelta(hours=12),
            allowable_methods=('GET',)
        )
        
        # Wikipedia cache - expire after 7 days
        self.wiki_session = requests_cache.CachedSession(
            str(self.cache_dir / 'wiki_cache'),
            backend='sqlite',
            expire_after=timedelta(days=7),
            allowable_methods=('GET',)
        )
        
        # Alpha Vantage cache - expire after 24 hours
        self.alpha_vantage_session = requests_cache.CachedSession(
            str(self.cache_dir / 'alphavantage_cache'),
            backend='sqlite',
            expire_after=timedelta(hours=24),
            allowable_methods=('GET',)
        )
        
        # Apply rate limiting to cached sessions
        self.yf_session = LimiterSession(per_second=1, session=self.yf_session)
        self.news_session = LimiterSession(per_second=0.5, session=self.news_session)
        self.wiki_session = LimiterSession(per_second=0.2, session=self.wiki_session)
        self.alpha_vantage_session = LimiterSession(per_second=0.2, session=self.alpha_vantage_session)
    
    def _save_rate_limit_state(self):
        """Save rate limit state to file"""
        try:
            state = {
                'rate_limit_hits': self.rate_limit_hits,
                'last_reset': self.last_rate_limit_reset.isoformat(),
                'last_requests': {
                    'yfinance': self.last_yf_request.isoformat(),
                    'newsapi': self.last_news_request.isoformat(),
                    'wikipedia': self.last_wiki_request.isoformat(),
                    'alphavantage': self.last_alpha_vantage_request.isoformat()
                }
            }
            
            with open(self.cache_dir / 'rate_limit_state.json', 'w') as f:
                json.dump(state, f)
        except Exception as e:
            logger.warning(f"Failed to save rate limit state: {str(e)}")
    
    def _load_rate_limit_state(self):
        """Load rate limit state from file"""
        try:
            state_file = self.cache_dir / 'rate_limit_state.json'
            if state_file.exists():
                with open(state_file, 'r') as f:
                    state = json.load(f)
                
                self.rate_limit_hits = state['rate_limit_hits']
                self.last_rate_limit_reset = datetime.fromisoformat(state['last_reset'])
                
                # Reset counters if more than a day has passed
                if (datetime.now() - self.last_rate_limit_reset).total_seconds() > 86400:  # 24 hours
                    self.rate_limit_hits = {k: 0 for k in self.rate_limit_hits}
                    self.last_rate_limit_reset = datetime.now()
        except Exception as e:
            logger.warning(f"Failed to load rate limit state: {str(e)}")
    
    def _apply_backoff(self, attempt, service_name):
        """Apply exponential backoff with jitter"""
        # Add jitter to prevent thundering herd problem
        import random
        jitter = random.uniform(0, 0.5)
        backoff = (self.base_backoff * (2 ** attempt)) + jitter
        
        logger.warning(f"{service_name} rate limit backoff: {backoff:.2f}s (attempt {attempt+1})")
        time.sleep(backoff)
    
    def _adaptive_delay(self, service_name, rate_limit_hit=False):
        """Adaptively adjust delay based on rate limit hits and queue size"""
        if rate_limit_hit:
            self.rate_limit_hits[service_name] += 1
            self._save_rate_limit_state()
        
        # Get current queue size factor (0 to 1)
        queue_size = self.request_queues[service_name].qsize()
        max_size = self.request_queues[service_name].maxsize
        queue_factor = queue_size / max_size if max_size > 0 else 0
        
        # Increase delay if we've hit rate limits multiple times
        hit_count = self.rate_limit_hits[service_name]
        base_delay = 1.5 ** (hit_count - 3) if hit_count > 3 else 0
        
        # Add queue-based delay component
        queue_delay = queue_factor * 2  # Up to 2 seconds additional delay based on queue size
        
        return base_delay + queue_delay
    
    def execute_yf_request(self, func, *args, **kwargs):
        """Execute YFinance API request with rate limiting and caching"""
        for attempt in range(self.max_retries):
            try:
                with self.yf_limiter.ratelimit('yfinance', delay=True):
                    current_time = datetime.now()
                    # Ensure minimum delay between requests
                    time_since_last = (current_time - self.last_yf_request).total_seconds()
                    min_delay = 1 + self._adaptive_delay('yfinance')
                    
                    if time_since_last < min_delay:
                        time.sleep(min_delay - time_since_last)
                    
                    # Check if we're using a session-based request or a direct function call
                    if kwargs.get('session') is None and hasattr(func, '__self__') and hasattr(func.__self__, 'session'):
                        # Replace the session with our cached session for yfinance objects
                        original_session = func.__self__.session
                        func.__self__.session = self.yf_session
                        result = func(*args, **kwargs)
                        func.__self__.session = original_session
                    else:
                        # Direct function call
                        result = func(*args, **kwargs)
                    
                    self.last_yf_request = datetime.now()
                    return result
            except Exception as e:
                error_msg = str(e).lower()
                is_rate_limit = any(x in error_msg for x in ["rate limit", "too many requests", "429"])
                
                if is_rate_limit and attempt < self.max_retries - 1:
                    logger.warning(f"YFinance rate limit hit, attempt {attempt + 1}/{self.max_retries}")
                    self._adaptive_delay('yfinance', rate_limit_hit=True)
                    self._apply_backoff(attempt, "YFinance")
                    continue
                elif attempt < self.max_retries - 1:
                    logger.warning(f"YFinance request failed with error: {str(e)}. Retrying...")
                    time.sleep(2)  # Simple delay for non-rate-limit errors
                    continue
                raise
    
    def execute_news_request(self, func_or_url, *args, **kwargs):
        """Execute NewsAPI request with rate limiting and caching"""
        for attempt in range(self.max_retries):
            try:
                with self.news_limiter.ratelimit('newsapi', delay=True):
                    current_time = datetime.now()
                    time_since_last = (current_time - self.last_news_request).total_seconds()
                    min_delay = 2 + self._adaptive_delay('newsapi')
                    
                    if time_since_last < min_delay:
                        time.sleep(min_delay - time_since_last)
                    
                    # Handle both function calls and direct URL requests
                    if isinstance(func_or_url, str):
                        # It's a URL, use our cached session
                        result = self.news_session.get(func_or_url, *args, **kwargs)
                    else:
                        # It's a function
                        result = func_or_url(*args, **kwargs)
                    
                    self.last_news_request = datetime.now()
                    return result
            except Exception as e:
                error_msg = str(e).lower()
                is_rate_limit = any(x in error_msg for x in ["rate limit", "too many requests", "429"])
                
                if is_rate_limit and attempt < self.max_retries - 1:
                    logger.warning(f"NewsAPI rate limit hit, attempt {attempt + 1}/{self.max_retries}")
                    self._adaptive_delay('newsapi', rate_limit_hit=True)
                    self._apply_backoff(attempt, "NewsAPI")
                    continue
                elif attempt < self.max_retries - 1:
                    logger.warning(f"NewsAPI request failed with error: {str(e)}. Retrying...")
                    time.sleep(2)
                    continue
                raise
    
    def execute_wiki_request(self, func_or_url, *args, **kwargs):
        """Execute Wikipedia request with rate limiting and caching"""
        for attempt in range(self.max_retries):
            try:
                with self.wiki_limiter.ratelimit('wikipedia', delay=True):
                    current_time = datetime.now()
                    time_since_last = (current_time - self.last_wiki_request).total_seconds()
                    min_delay = 5 + self._adaptive_delay('wikipedia')
                    
                    if time_since_last < min_delay:
                        time.sleep(min_delay - time_since_last)
                    
                    # Handle both function calls and direct URL requests
                    if isinstance(func_or_url, str):
                        # It's a URL, use our cached session
                        result = self.wiki_session.get(func_or_url, *args, **kwargs)
                    else:
                        # It's a function
                        result = func_or_url(*args, **kwargs)
                    
                    self.last_wiki_request = datetime.now()
                    return result
            except Exception as e:
                error_msg = str(e).lower()
                is_rate_limit = any(x in error_msg for x in ["rate limit", "too many requests", "429"])
                
                if is_rate_limit and attempt < self.max_retries - 1:
                    logger.warning(f"Wikipedia rate limit hit, attempt {attempt + 1}/{self.max_retries}")
                    self._adaptive_delay('wikipedia', rate_limit_hit=True)
                    self._apply_backoff(attempt, "Wikipedia")
                    continue
                elif attempt < self.max_retries - 1:
                    logger.warning(f"Wikipedia request failed with error: {str(e)}. Retrying...")
                    time.sleep(2)
                    continue
                raise
    
    def execute_alpha_vantage_request(self, func_or_url, *args, **kwargs):
        """Execute Alpha Vantage API request with rate limiting and caching"""
        for attempt in range(self.max_retries):
            try:
                with self.alpha_vantage_limiter.ratelimit('alphavantage', delay=True):
                    current_time = datetime.now()
                    time_since_last = (current_time - self.last_alpha_vantage_request).total_seconds()
                    min_delay = 15 + self._adaptive_delay('alphavantage')
                    
                    if time_since_last < min_delay:
                        time.sleep(min_delay - time_since_last)
                    
                    # Handle both function calls and direct URL requests
                    if isinstance(func_or_url, str):
                        # It's a URL, use our cached session
                        result = self.alpha_vantage_session.get(func_or_url, *args, **kwargs)
                    else:
                        # It's a function
                        result = func_or_url(*args, **kwargs)
                    
                    self.last_alpha_vantage_request = datetime.now()
                    return result
            except Exception as e:
                error_msg = str(e).lower()
                is_rate_limit = any(x in error_msg for x in ["rate limit", "too many requests", "429"])
                
                if is_rate_limit and attempt < self.max_retries - 1:
                    logger.warning(f"Alpha Vantage rate limit hit, attempt {attempt + 1}/{self.max_retries}")
                    self._adaptive_delay('alphavantage', rate_limit_hit=True)
                    self._apply_backoff(attempt, "Alpha Vantage")
                    continue
                elif attempt < self.max_retries - 1:
                    logger.warning(f"Alpha Vantage request failed with error: {str(e)}. Retrying...")
                    time.sleep(2)
                    continue
                raise
    
    def get_cached_session(self, api_type):
        """Get the appropriate cached session based on API type"""
        if api_type.lower() == 'yfinance':
            return self.yf_session
        elif api_type.lower() == 'newsapi':
            return self.news_session
        elif api_type.lower() == 'wikipedia':
            return self.wiki_session
        elif api_type.lower() == 'alphavantage':
            return self.alpha_vantage_session
        else:
            raise ValueError(f"Unknown API type: {api_type}")
    
    def clear_cache(self, api_type=None):
        """Clear cache for specific API or all APIs"""
        try:
            if api_type is None:
                # Clear all caches
                self.yf_session.cache.clear()
                self.news_session.cache.clear()
                self.wiki_session.cache.clear()
                self.alpha_vantage_session.cache.clear()
                logger.info("Cleared all API caches")
            else:
                # Clear specific cache
                session = self.get_cached_session(api_type)
                session.cache.clear()
                logger.info(f"Cleared {api_type} cache")
        except Exception as e:
            logger.error(f"Failed to clear cache: {str(e)}")
    
    def _process_queue(self, service_name: str):
        """Process requests from the queue for a specific service"""
        while not self.stop_events[service_name].is_set():
            try:
                # Get request from queue with timeout
                request = self.request_queues[service_name].get(timeout=1)
                if request is None:
                    continue
                    
                func, args, kwargs, future = request
                try:
                    # Execute request with appropriate limiter
                    if service_name == 'yfinance':
                        result = self.execute_yf_request(func, *args, **kwargs)
                    elif service_name == 'newsapi':
                        result = self.execute_news_request(func, *args, **kwargs)
                    elif service_name == 'wikipedia':
                        result = self.execute_wiki_request(func, *args, **kwargs)
                    elif service_name == 'alphavantage':
                        result = self.execute_alpha_vantage_request(func, *args, **kwargs)
                        
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
                    
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing {service_name} queue: {str(e)}")
                
    def get_cache_stats(self):
        """Get cache statistics for all APIs"""
        stats = {}
        try:
            for service_name in ['yfinance', 'newsapi', 'wikipedia', 'alphavantage']:
                session = getattr(self, f"{service_name.replace('yfinance', 'yf')}_session")
                stats[service_name] = {
                    'size': len(session.cache),
                    'hits': session.cache.stats['hits'],
                    'misses': session.cache.stats['misses'],
                    'queue_size': self.request_queues[service_name].qsize(),
                    'queue_capacity': self.request_queues[service_name].maxsize
                }
        except Exception as e:
            logger.error(f"Failed to get cache stats: {str(e)}")
        
        return stats