import requests
import json

URL = 'https://www.quandl.com/api/v3/datasets/NSE/'
API_KEY = 'api_key=gjvA_eMsUo-CqbtLsq3m'


def get_data(stock_code='', start_date='', end_date=''):
    """Get data from Quandl"""

    quandl_data=''

    bhavdata = json.load(open('./stock-code-15-dec-2017.json', 'r'))
    if stock_code.upper() in bhavdata:
        prepared_url = URL + stock_code.upper() + '?' + API_KEY
        print(prepared_url)
        response=requests.get(prepared_url)
        print(response)
        
        quandl_data=json.loads(response.content.decode('utf-8'))
    
    if (quandl_data!=None) | (quandl_data!=''):
        if 'dataset' in quandl_data:
            if 'data' in quandl_data['dataset']:
                outfile=open('data.txt', 'w+')
                outfile.writelines(json.dumps(quandl_data))
                outfile.close()

    return

# https://www.quandl.com/api/v3/datasets/NSE/INFY?start_date=2010-01-12&end_date=2017-12-15&api_key=gjvA_eMsUo-CqbtLsq3m


get_data(stock_code='MAHLOG',start_date='b',end_date='c')