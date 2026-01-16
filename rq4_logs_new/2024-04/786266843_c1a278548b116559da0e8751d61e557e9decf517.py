import requests
import json
import csv
from pigpio import *
from time import sleep
from random import randint
import datetime

#sw_flg = False

# エリアコード
area_dic_all = {'北海道/釧路':'014100',
            '北海道/旭川':'012000',
            '北海道/札幌':'016000',
            '青森県':'020000',
            '岩手県':'030000',
            '宮城県':'040000',
            '秋田県':'050000',
            '山形県':'060000',
            '福島県':'070000',
            '茨城県':'080000',
            '栃木県':'090000',
            '群馬県':'100000',
            '埼玉県':'110000',
            '千葉県':'120000',
            '東京都':'130000',
            '神奈川県':'140000',
            '新潟県':'150000',
            '富山県':'160000',
            '石川県':'170000',
            '福井県':'180000',
            '山梨県':'190000',
            '長野県':'200000',
            '岐阜県':'210000',
            '静岡県':'220000',
            '愛知県':'230000',
            '三重県':'240000',
            '滋賀県':'250000',
            '京都府':'260000',
            '大阪府':'270000',
            '兵庫県':'280000',
            '奈良県':'290000',
            '和歌山県':'300000',
            '鳥取県':'310000',
            '島根県':'320000',
            '岡山県':'330000',
            '広島県':'340000',
            '山口県':'350000',
            '徳島県':'360000',
            '香川県':'370000',
            '愛媛県':'380000',
            '高知県':'390000',
            '福岡県':'400000',
            '佐賀県':'410000',
            '長崎県':'420000',
            '熊本県':'430000',
            '大分県':'440000',
            '宮崎県':'450000',
            '鹿児島県':'460100',
            '沖縄県/那覇':'471000',
            '沖縄県/石垣':'474000'
            }

area_dic ={'石川県':'170000'}

def get_info():
    write_lists = []
    base_url = "https://www.jma.go.jp/bosai/forecast/data/forecast/"
    for k, v in area_dic.items():

        if k.find("/"):
            prefecture = k[0:k.find("/")]
        else:
            prefecture = k

        url = base_url + v + ".json"

        res = requests.get(url).json()

        for re in res:
            publishingOffice = re["publishingOffice"]
            reportDatetime = re["reportDatetime"]

            timeSeries = re["timeSeries"]

            for time in timeSeries:
                #降水確率など今回のターゲット以外は除外する
                if 'pops' in time["areas"][0]:
                    pass
                elif 'temps' in time["areas"][0]:
                    pass
                elif 'tempsMax' in time["areas"][0]:
                    pass
                else:
                    for i in range(len(time["areas"])):

                        local_name = time["areas"][i]["area"]["name"]

                        for j in range(len(timeSeries[0]["timeDefines"])):

                            if 'weathers' not in time["areas"][i]:
                                weather = ""
                            else:
                                weather = time["areas"][i]["weathers"][j]

                            if 'winds' not in time["areas"][i]:
                                wind = ""
                            else:
                                wind = time["areas"][i]["winds"][j]

                            if 'waves' not in time["areas"][i]:
                                wave = ""
                            else:
                                wave = time["areas"][i]["waves"][j]

                            timeDefine = time["timeDefines"][j]

                            # 各情報をリストに格納
                            write_list = []
                            write_list.append(prefecture)
                            write_list.append(publishingOffice)
                            write_list.append(reportDatetime)
                            write_list.append(local_name)
                            write_list.append(timeDefine)
                            write_list.append(weather)
                            write_list.append(wind)
                            write_list.append(wave)

                            write_lists.append(write_list)
    return write_lists

def write_weatherPara(file_path,para):
    with open(file_path, 'w') as file:
        file.write(para)

def sw0_callback(pin_, level, tick):
    global sw_flg
    sw_flg = True

def main():
    pin, mode = 5, INPUT
    gpio = pi()
    gpio.set_mode(pin, mode)
    gpio.set_pull_up_down(pin, PUD_UP)
    gpio.set_glitch_filter(pin, 50) #50ミリ秒以内の変化を無視
    gpio.callback(pin, FALLING_EDGE, sw0_callback)

    global sw_flg
    sw_flg = False
    
    file_path = "/sys/module/smile/parameters/msg"
    file_path_flg = "/sys/module/smile/parameters/flg" 
    file_path_time = "/sys/module/smile/parameters/time"

    while True:
        print(sw_flg)
        if sw_flg:
            write_weatherPara(file_path_flg, "1")
            dt_now = datetime.datetime.now()#現在時刻取得
            time_str = str(dt_now.strftime('%H:%M:%S'))
            print(time_str)
            write_weatherPara(file_path_time, time_str)

            weather_lists = get_info()
            weather_list = weather_lists[0][5].split("　")
            for i in weather_list:
                if "晴れ" in i:
                    print("Sunny")
                    write_weatherPara(file_path, "Sunny")
                    break
                elif "くもり" in i:
                    print("Cloudy")
                    write_weatherPara(file_path, "Cloudy")
                    break
                elif "雨" in i:
                    print("Rain")
                    write_weatherPara(file_path, "Rain")
                    break
                elif "雷" in i:
                    print("Thunder")
                    write_weatherPara(file_path, "Thunder")
                    break

        sw_flg = False
        write_weatherPara(file_path_flg, "0")
        sleep(1)

if __name__ == '__main__':
    main()