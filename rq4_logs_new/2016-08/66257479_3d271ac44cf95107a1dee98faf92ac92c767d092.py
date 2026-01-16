# -*- coding:utf-8 -*-
import sys
import requests
import xmltodict
import json
import csv
import yaml
from easydict import EasyDict

reload(sys)
sys.setdefaultencoding('utf-8')

naver_api_url = 'http://openapi.naver.com/search'
naver_api_key = 'c1b406b32dbbbbeee5f2a36ddc14067f '
config_path = './configs.yaml'

def _utf8(v):
    if isinstance(v, unicode):
        v = v.encode('utf-8')
    return v


def _naver_shop_search(params):
    payload = dict()
    for key, value in params.iteritems():
        payload[key] = _utf8(value)
    response = requests.get(naver_api_url, params=payload)
    print response.url

    return response.status_code, response.text


def get_naver_shop_code(conf):
    result = dict()
    params = {
        "target": "shop",
        "key": naver_api_key,
        "display": 20,
        "sort": "sim",
        "start": 1,
    }
    for code in conf.product_codes:
        params["query"] = code
        status, body = _naver_shop_search(params)
        if status == 200:
            data = json.loads(json.dumps(xmltodict.parse(body)))
            result[code] = ''
            items = data['rss']['channel']['item']
            if isinstance(items, list):
                for item in items:
                    if item["mallName"].decode('utf-8')  == '데상트코리아':
                        result[code] = item['productId']
            else:
                if items["mallName"].decode('utf-8')  == '데상트코리아':
                    result[code] = items['productId']

    return result


def create_csv_file(filename, data):
    with open(filename, 'w') as csvfile:
        fieldnames = ['descent_code', 'naver_code']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for key in data.keys():
            writer.writerow({"descent_code": key, "naver_code": data[key]})


if __name__ == '__main__':
    with open(config_path, 'r') as f:
        doc = yaml.load(f)
        conf = EasyDict(doc)

    codes = get_naver_shop_code(conf)
    create_csv_file("result.csv", codes)