import requests
import json
import pandas as pd
from pprint import pprint

res = requests.get('https://app.rakuten.co.jp/services/api/Recipe/CategoryList/20170426?applicationId=1080212569234422409')

json_data = json.loads(res.text)
pprint(json_data)

# mediumカテゴリの親カテゴリの辞書
parent_dict = {}

df = pd.DataFrame(columns=['category1', 'category2', 'category3', 'categoryId', 'categoryName'])

# 大カテゴリ
for category in json_data['result']['large']:
    data = {'category1': category['categoryId'], 'category2': "", 'category3': "", 'categoryId': category['categoryId'], 'categoryName': category['categoryName']}
    df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)

# 中カテゴリ
for category in json_data['result']['medium']:
    data = {'category1': category['parentCategoryId'], 'category2': category['categoryId'], 'category3': "", 'categoryId': str(category['parentCategoryId']) + "-" + str(category['categoryId']), 'categoryName': category['categoryName']}
    df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
    parent_dict[str(category['categoryId'])] = category['parentCategoryId']

# DataFrameをdata.jsonファイルに出力
df.to_json('data.json', orient='records', force_ascii=False)

