# 法人番号検索APIを叩いてcsvに出力する
# とりあえず指定日付をとるかんじ
# ToDo: パラメータは引数で与えるようにしたい
# ToDo: 法人番号を指定する場合とかも作る必要がある

import requests
import io
import yaml
import csv
import time

def download_csv(api_url, payload):
    # リクエストを投げる
    res = requests.get(api_url, params=payload)

    # 1行目にデータの分割数とかの情報があるので、pandasではなくcsvを使う
    reader = csv.reader(io.StringIO(res.text))

    # ファイル名に入れる日付
    file_date = payload["to"].replace("-", "") # とりあえず1日分ずつ取る想定なので

    # 1行目から分割数を取得（2分割以上だった場合はリクエストを投げなおす必要がある
    line1 = next(reader) # readerはイテレータなので2行目以降だけになる
    sep_cnt = line1[2]
    sep_num = line1[3]

    # csvの新しいheader
    new_header = [
        "sequenceNumber", "corporateNumber", "process", "correct", "updateDate"
        , "changeDate", "name", "nameImageId", "kind", "prefectureName"
        , "cityName", "streetNumber", "addressImageId", "prefectureCode", "cityCode"
        , "postCode", "addressOutside", "addressOutsideImageId", "closeDate", "closeCause"
        , "successorCorporateNumber", "changeCause", "assignmentDate", "latest", "enName"
        , "enPrefectureName", "enCityName", "enAddressOutsid", "furigana", "hihyoji"
    ]

    # 1行目を書き換えて出力
    with open(f"./output/result_{file_date}_{sep_cnt}.csv", "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(new_header)
        writer.writerows(reader) # 2行目以降を書き込む

    return int(sep_num)

if __name__ == "__main__":
    # yaml読み込み
    with open("./conf/config.yml") as f:
        conf = yaml.safe_load(f)

    # api
    api_url = conf["default"]["api_url"]
    api_key = conf["default"]["api_key"]

    # パラメータの設定
    # とりあえず1日分ずつ取る想定
    # 取得方法によってpayloadを変える関数を作るのがよさげ？
    target_date = "2020-05-07" # ここは引数で制御予定

    payload = {
        "id": api_key,
        "from": target_date,
        "to": target_date,
        "type": "02", # utf-8のcsvで固定
        "divide": 1 # 分割数が2以上の場合、ここを変えて再度リクエストする
    }

    # csvのダウンロード
    sep_num = download_csv(api_url, payload)

    # 2分割以上の場合は再度リクエストと出力を行う
    if sep_num > 1:
        for i in range(2, sep_num + 1):
            time.sleep(5)
            payload["divide"] = i
            download_csv(api_url, payload)