import datetime

import pandas as pd
import numpy as np
from sqlalchemy.engine.default import DefaultExecutionContext

# [TODO]問題なければ削除する

# def convert_render_price_rank_data(context) -> dict|None:
#     params = context.get_current_parameters()
#     rank_data = params.get('rank_data')
#     price_data = params.get('price_data')
#     if rank_data is None or price_data is None:
#         return None
#     rank_dict = {convert_keepa_time_to_datetime_date(int(k)): v for k, v in rank_data.items()}
#     price_dict = {convert_keepa_time_to_datetime_date(int(k)): v for k, v in price_data.items()}

#     rank_df = pd.DataFrame(data=list(rank_dict.items()), columns=['date', 'rank']).astype({'rank': int})
#     price_df = pd.DataFrame(data=list(price_dict.items()), columns=['date', 'price']).astype({'price': int})

#     df = pd.merge(rank_df, price_df, on='date', how='outer')
#     df = df.replace(-1.0, np.nan)
#     df = df.fillna(method='ffill')
#     df = df.fillna(method='bfill')
#     df = df.replace([np.nan], [None])
#     delay = datetime.datetime.now().date() - datetime.timedelta(days=90)
#     df = df[df['date'] > delay]
#     df = df.sort_values('date', ascending=True)
#     products = {'date': list(map(lambda x: x.isoformat(), df['date'].to_list())), 
#                 'rank': df['rank'].to_list(), 
#                 'price': df['price'].to_list()}

#     return products

def recharts_data(context: dict|DefaultExecutionContext) -> dict|None:
    if isinstance(context, DefaultExecutionContext):
        params = context.get_current_parameters()
    elif isinstance(context, dict):
        params = context
    else:
        raise Exception

    rank_data = params.get('rank_data')
    price_data = params.get('price_data')
    if rank_data is None or price_data is None:
        return None

    today = datetime.datetime.now().date()
    start_date = today - datetime.timedelta(days=90)
    end_date = today
    date_index = pd.date_range(start_date, end_date)
    date_index_df = pd.DataFrame(data=date_index, columns=['date'])
    date_index_df = date_index_df['date'].dt.date

    price_df = pd.DataFrame(data=price_data.items(), columns=['date', 'price']).astype({'date': int, 'price': int})
    price_df['date'] = price_df['date'].map(keepa_time_to_datetime_date)
    rank_df = pd.DataFrame(data=rank_data.items(), columns=['date', 'rank']).astype({'date': int, 'rank': int})
    rank_df['date'] = rank_df['date'].map(keepa_time_to_datetime_date)

    df = pd.merge(date_index_df, price_df, on='date', how='outer')
    df = pd.merge(df, rank_df, on='date', how='outer')
    df = df.replace(-1.0, np.nan)
    df = df.sort_values('date', ascending=True)
    df = df.fillna(method='ffill')
    df = df.fillna(method='bfill')
    df = df.replace([np.nan], [None])
    df = df[df['date'] > start_date]
    df = df.sort_values('date', ascending=True)
    df['date'] = df['date'].map(lambda x: x.strftime('%Y-%m-%d'))
    data = df.to_dict(orient='records')

    return {'data': data}

def keepa_time_to_datetime_date(keepa_time: int):
    unix_time = (keepa_time + 21564000) * 60
    date_time = datetime.datetime.fromtimestamp(unix_time)
    return date_time.date()


def unix_time_to_keepa_time(unix_time: float) -> str:
    keepa_time = round(unix_time / 60 - 21564000)
    return str(keepa_time)