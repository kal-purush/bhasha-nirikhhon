# coding:utf-8
import json
import os, glob, pickle

# useful functions
def json2dict(filename,key):
    """json2dict _summary_
    jsonファイルをdict型に変換し、特定のkeyのデータを返す

    Args:
        filename (String): ファイル名
        key (String): jsonのkey

    Returns:
        list: [{店舗情報のdict},{},...]
    """
    with open(filename, 'r') as f:
        data = json.load(f)
    return data[key]

def getfiles(dir="./groulette_backend/restaurantData/"):
    files = glob.glob(dir+"*.json")
    return files

if __name__ == '__main__':
    path = './groulette_backend/restaurantData/'
    save_path = './groulette_backend/restaurantPlaceid/'
    files = getfiles()
    key = 'results'
    for filename in files:
        res = json2dict(filename, key)
        ids = [store['place_id'].encode('ascii') for store in res]
        # ids = [store['place_id'] for store in res]
        save_filename = os.path.splitext(filename)[0].split('/')[-1]
        with open(save_path+save_filename+'.txt', 'wb') as f:
            for id in ids:
                f.write("%s\n" % id)
    


