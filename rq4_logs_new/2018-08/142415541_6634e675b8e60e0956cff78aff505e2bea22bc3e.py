#bt_model.RでbashoId読み込むようになってない
#201801でoutputに十両の碧山がいる
#不戦勝を消すんじゃなく、0, 1以外にしといて、あとから消したり使ったりできるようにする
import pandas as pd
import re
import sys
import pyper
from time import sleep



def get_torikumi_data():
    print('get_torikumi_data')
    url_base = 'https://sports.yahoo.co.jp/sumo/torikumi/stats?'
    for day in range(1, 16):
        url = url_base+f'bashoId={bashoId}&day={day}'
        fetched_dataframes = pd.read_html(url)
        day_torikumi = fetched_dataframes[1]
        day_torikumi = cleansing(day_torikumi)
        df2csv(url, day_torikumi)
        print(f'basho:{bashoId}   day:{day}')
        sleep(3)


def cleansing(day_torikumi):
    day_torikumi.columns = ['e_banduke', 'e_rikishi', 'e_win', 'kimarite', 'w_win', 'w_rikishi', 'w_banduke']
    num = '[0-9]'
    for col in range(day_torikumi.shape[0]):
        torikumi = day_torikumi.iloc[col]

        num_str = re.search(num, torikumi['e_rikishi'])
        torikumi['e_rikishi'] = torikumi['e_rikishi'][:num_str.start()]

        torikumi['e_win'] = ord(torikumi['e_win'])
        if torikumi['e_win'] == 9675:
            torikumi['e_win'] = 1
        elif torikumi['e_win'] == 9679:
            torikumi['e_win'] = 0
        elif torikumi['e_win'] == 9632 or torikumi['e_win'] == 9633:
            torikumi['e_win'] = -1

        torikumi['w_win'] = ord(torikumi['w_win'])
        if torikumi['w_win'] == 9675:
            torikumi['w_win'] = 1
        elif torikumi['w_win'] == 9679:
            torikumi['w_win'] = 0
        elif torikumi['w_win'] == 9632 or torikumi['w_win'] == 9633:
            torikumi['w_win'] = -1

        num_str = re.search(num, torikumi['w_rikishi'])
        torikumi['w_rikishi'] = torikumi['w_rikishi'][:num_str.start()]

    return day_torikumi


def df2csv(url, day_torikumi):
    num = '[0-9]'
    num_str = re.search(num, url)
    day_torikumi.to_csv(f'input/torikumi_data/{url[num_str.start():]}.csv')



def transform_data_for_btmodel():
    print('transform_data_for_btmodel')
    for day in range(1, 16):
        fname = f'{bashoId}&day={day}'
        day_torikumi = pd.read_csv(f'input/torikumi_data/{fname}.csv', index_col=0)

        day_torikumi = day_torikumi.drop(['e_banduke', 'kimarite', 'w_banduke'], axis=1)
        day_torikumi = day_torikumi.loc[:, ['e_rikishi', 'w_rikishi', 'e_win', 'w_win']]

        day_torikumi = day_torikumi[day_torikumi['e_win'] != -1]

        # for same level in BradleyTerry2
        day_torikumi_inv = day_torikumi.loc[:, ['w_rikishi', 'e_rikishi', 'w_win', 'e_win']]
        day_torikumi_inv.columns = ['e_rikishi', 'w_rikishi', 'e_win', 'w_win']
        day_torikumi = day_torikumi.append(day_torikumi_inv)

        day_torikumi.to_csv(f'input/torikumi_btmodel_data/{fname}_bt.csv')


        if day == 1:
            basho_torikumi = day_torikumi
        else:
            basho_torikumi = basho_torikumi.append(day_torikumi)
    basho_torikumi = basho_torikumi.reset_index(drop=True)

    # basho_torikumi.to_csv(f'input/torikumi_btmodel_data/{fname[:6]}_bt_all.csv')
    # 最終日に出場した力士のみに限定（休場力士を除外）
    not_kyujo_rikishi = day_torikumi['e_rikishi']
    nkrl = not_kyujo_rikishi.values.tolist()
    basho_torikumi = basho_torikumi[basho_torikumi['e_rikishi'].isin(nkrl)]
    basho_torikumi = basho_torikumi[basho_torikumi['w_rikishi'].isin(nkrl)]
    basho_torikumi = basho_torikumi.reset_index(drop=True)
    basho_torikumi.to_csv(f'input/torikumi_btmodel_data/{fname[:6]}_bt_rm_kyujo.csv')



def bradley_terry():
    print('bradley_terry')
    r = pyper.R()
    r("source(file='bt_model.R')")



def visualize():
    print('visualize')
    # bt_ability = pd.read_csv(f'input/bt_ability/{bashoId}_bt_ability_all.csv', index_col=0)
    # basho_torikumi = pd.read_csv(f'input/torikumi_btmodel_data/{bashoId}_bt_all.csv', index_col=0)
    bt_ability = pd.read_csv(f'input/bt_ability/{bashoId}_bt_ability_rm_kyujo.csv', index_col=0)
    basho_torikumi = pd.read_csv(f'input/torikumi_btmodel_data/{bashoId}_bt_rm_kyujo.csv', index_col=0)

    grouped = basho_torikumi.groupby('e_rikishi')
    n_win = grouped.sum()['e_win']
    rikishi_status = pd.concat([bt_ability, n_win], axis=1)

    #for rikishi_status_all.csv
    # rikishi_status = rikishi_status[grouped.sum()['e_win'] + grouped.sum()['w_win'] >= 10]

    rikishi_status = rikishi_status.sort_values('ability', ascending = False)
    rikishi_status.columns = ['bt_ability', 'bt_s.e.', 'wins']

    # rikishi_status.to_csv(f'output/{bashoId}_rikishi_status_all.csv')
    rikishi_status.to_csv(f'output/{bashoId}_rikishi_status_rm_kyujo.csv')






if __name__ == '__main__':
    bashoId = '201801'
    # remove_kyujo = True
    # get_torikumi_data()
    # transform_data_for_btmodel()
    bradley_terry()
    visualize()