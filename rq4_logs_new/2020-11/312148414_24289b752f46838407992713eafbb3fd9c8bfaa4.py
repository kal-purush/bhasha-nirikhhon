import uvicorn
from fastapi import FastAPI
from fastapi.responses import Response
from aiohttp import ClientSession


API_KEY = '6b58504c8amshc0633fd4cf70e74p144bf4jsnc5df41eae577'
API_HOST = 'bloomberg-market-and-financial-news.p.rapidapi.com'
BLOOMBERG_URL = 'https://bloomberg-market-and-financial-news.p.rapidapi.com/'

app = FastAPI()


@app.get('/financial/{stock}')
async def getFinancialData(stock: str):
    headers = {'x-rapidapi-key': API_KEY,
               'x-rapidapi-host': API_HOST}
    query = {'id': f'{stock.lower()}:ij'}

    client = ClientSession()
    async with client.get(f'{BLOOMBERG_URL}/stock/get-financials', headers=headers, params=query) as resp:
        response = await resp.json()
        if 'result' not in response:
            return Response('Invalid Stock', 406)
        else:
            return response

@app.get('/statistic/{stock}')
async def getStatisticData(stock: str):
    headers = {'x-rapidapi-key': API_KEY,
               'x-rapidapi-host': API_HOST}
    query = {'id': f'{stock.lower()}:ij'}

    client = ClientSession()
    async with client.get(f'{BLOOMBERG_URL}/stock/get-statistics', headers=headers, params=query) as resp:
        response = await resp.json()
        if len(response['result'][0]['table']) < 1:
            return Response('Invalid Stock', 406)
        else:
            return response


uvicorn.run(app, loop='asyncio')