###############################################################################
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, TITLE AND
# NON-INFRINGEMENT. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR ANYONE
# DISTRIBUTING THE SOFTWARE BE LIABLE FOR ANY DAMAGES OR OTHER LIABILITY,
# WHETHER IN CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

import logging
import requests
import json
import time
import os
import datetime as dt
import sys

MARKET_JSON = 'markets.json'
DB_ROOT_FOLDER = 'markets_db'
MARKET_COUNTRY = 'LON' # See AlphaVantage for this
TIMEOUT = 10

logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s: %(message)s")

def get_historic_price(marketId, function, interval, apiKey):
    url = 'https://www.alphavantage.co/query?function={}&symbol={}&interval={}&apikey={}'.format(function, marketId, interval, apiKey)
    data = requests.get(url)
    return json.loads(data.text)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        logging.error("Wrong number of arguments")
        exit()

    # Read input arguments
    AV_FUNCTION = sys.argv[1]
    AV_INTERVAL = sys.argv[2]
    AV_APIKEY = ''
    try:
        # Read AlphaVantage API KEY
        with open('.api_key', 'r') as f:
            key = f.readLine()
            AV_APIKEY = key
    except IOError:
        logging.error(".api_key file not found!")
        exit()

    logging.info("MarketDataCollector started collection {} {}".format(AV_FUNCTION, AV_INTERVAL))
    try:
        # Create db root folder if does not exist
        os.makedirs(DB_ROOT_FOLDER, exist_ok=True)
        with open(MARKET_JSON, 'r') as fileReader:
            markets = json.load(fileReader)
            logging.info("Markets json loaded")
            for market in markets:
                # Extract market Id
                marketId = market['instrument']['marketId']
                # Convert the string for alpha vantage
                marketIdAV = '{}:{}'.format(MARKET_COUNTRY, marketId.split('-')[0])
                # Fetch historic price
                data = get_historic_price(marketIdAV, AV_FUNCTION, AV_INTERVAL, AV_APIKEY)
                # Safety check
                if 'Error Message' in data or 'Information' in data:
                    logging.error("Skipping {}".format(marketId))
                    continue
                # Build market folder tree
                marketFolder = '{}/{}/{}'.format(DB_ROOT_FOLDER, marketId, AV_INTERVAL)
                os.makedirs(marketFolder, exist_ok=True)
                # Get timestamp
                time_str = dt.datetime.now().isoformat()
                ts = time_str.replace(':', '_').replace('.', '_')
                # Write on db
                with open('{}/{}_{}.json'.format(marketFolder, marketId, ts), 'w') as fileWriter:
                    json.dump(data, fileWriter)
                    logging.info("Market {} processed succesfully".format(marketId))
                time.sleep(TIMEOUT)
            logging.info("Process complete")
    except Exception as e:
        logging.error(e)