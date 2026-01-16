import pandas as pd
from BilancoParse.constants import *

stocks = Bist30Stocks
period = '2018_2'
periods = ['2018_1', '2018_2', '2018_3']

d = []
for stock in stocks:
    for period in periods:
        try:
            path = '/home/cem/Desktop/diptoplamChecks/{}_{}_PartB.pkl'.format(stock, period)

            df = pd.read_pickle(path)
        except Exception as e:
            print('{} {} bulunamadi.'.format(stock, period))
            continue
        SubCodeUnique = set(df['LastSubCode'].values)

        mistakeSubCodes = []
        for i in SubCodeUnique:
            if type(i) == str:
                j = int(i[:-1])
                subTotal = sum(df.loc[df['LastSubCode'] == i, 'colDipToplam'])
                checkTotal = df.loc[df['tableindex'] == j, 'colDipToplam'].values

                if checkTotal[0] - subTotal != 0:
                    print(i)
                    print(checkTotal)
                    print(subTotal)
                    mistakeSubCodes.append(i)
                else:
                    print('{} => Dogru, checkTotal => {} subTotal=> {}'.format(i, checkTotal, subTotal))

        if len(mistakeSubCodes) > 0:
            d.append({'period': period, 'stock': stock, 'mistakes': mistakeSubCodes})
            print('{} yanlis calculation bulundu', stock)