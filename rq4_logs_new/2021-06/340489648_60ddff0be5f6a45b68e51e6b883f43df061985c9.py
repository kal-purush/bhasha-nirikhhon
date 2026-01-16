import requests
import time


MIN_DELAY_BETWEEN_CALLS = 60 / 1000

class RegulationsAPI:
    """
    Wrapper for making calls to the reguations.gov API.

    The class handles attaching the API key to the parameters
    and it adds a delay between calls to ensure less than 1000
    calls per hour.
    """

    def __init__(self, api_key):
        self.last_time_called = None
        self.api_key = api_key

    def download(self, url, params={}):
        if self.time_since_last_call() < MIN_DELAY_BETWEEN_CALLS:
            time.sleep(MIN_DELAY_BETWEEN_CALLS)
        params['api_key'] = self.api_key
        result = requests.get(url, params=params)
        result.raise_for_status()
        return result.json()

    def time_since_last_call(self):
        if self.last_time_called is None:
            self.last_time_called = time.time()
            return float('inf')
        last = self.last_time_called
        now = time.time()
        self.last_time_called = now
        return now - last