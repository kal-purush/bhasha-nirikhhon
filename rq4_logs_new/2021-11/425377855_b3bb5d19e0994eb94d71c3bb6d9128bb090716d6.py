#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timezone
import statsmodels.formula.api as smf
from sklearn.model_selection import train_test_split
import statsmodels.api as sm

def splitStringDate(string):
    string = str(string)
    return pd.to_datetime(f"{string[0:4]}.{string[4:6]}.{string[6:8]}.{string[8:10]}")

def splitStringDate2(string):
    string = str(string)
    return pd.to_datetime(f"{string[0:4]}.{string[5:7]}.{string[8:10]}.{string[11:13]}")

def splitStringDate3(string):
    string = str(string)
    return pd.to_datetime(f"{string[0:4]}.{string[5:7]}.{string[8:10]}")

def splitStringDate4(string):
    string = str(string)
    return pd.to_datetime(f"{string[0:4]}.{string[4:6]}.{string[6:8]}")


def make_regression(df):

    X_train, X_test = train_test_split(df, test_size=0.33, random_state=12)

    X_train.plot(x='temp', y='velos', kind='scatter')
    mod = smf.ols(formula='velos ~ Arbeitstag + sonne * temp + regen + np.power(regen,2) + schnee + sonne*regen + np.power(temp,2)', data=X_train)
#    mod = smf.ols(formula='velos ~ temp + np.power(temp,2)', data=X_train)
    res = mod.fit()
    print(res.summary())

    X_test = sm.add_constant(X_test)
    ynewpred = res.predict(X_test)

    rms_test = np.sqrt(np.mean(np.square(ynewpred - X_test.velos)))
    print(rms_test)
    '''
    fig, ax = plt.subplots()
    ax.plot(X_test['temp'], X_test['velos'], 'o', label='Test Data')
    ax.plot(np.linspace(-15,30,100), np.linspace(-15,30,100)*res.params['temp'] + res.params['Intercept'], label='Learned Model')
    ax.legend(loc="best")
    ax.set_title('Velos related to Temperature')

    fig, ax = plt.subplots()
    ax.plot(X_test['temp'], X_test['velos'], 'o', label='Test Data')
    ax.plot(np.linspace(-15,30,100), np.linspace(-15,30,100)*(res.params['temp'] + np.linspace(0,30,100) * res.params['np.power(temp, 2)'])  + res.params['Intercept'], label='Learned Model')
    ax.legend(loc="best")
    ax.set_title('Velos related to Temperature')
    '''
    return res, rms_test


wetter_hour_df = pd.read_csv('Wetter_Daten_Stündlich.csv',sep=";")
wetter_hour_df =  wetter_hour_df[wetter_hour_df['stn']=='STG'].copy(deep=True)
wetter_hour_df["date"] = wetter_hour_df["time"].apply(splitStringDate)
wetter_hour_df



wetter_daily_df = pd.read_csv('Wetter_Daten_Täglich.csv',sep=";")
wetter_daily2_df = pd.read_csv('Wetter_Daten_Täglich2.csv',sep=";")
superwetter = pd.concat([wetter_daily_df, wetter_daily2_df])
superwetter =  superwetter[superwetter['stn']=='STG'].copy(deep=True)
superwetter["date"] = superwetter["time"].apply(splitStringDate)


stgallen_df =  wetter_hour_df[wetter_hour_df['stn']=='STG']
df_daily_Wetter = stgallen_df.iloc[range(0,stgallen_df.shape[0],24)]
df_daily_Wetter


wetter_temp_df = pd.read_csv('Wetter_Temperatur.csv',sep=";")

stg_temp_df =  wetter_temp_df[wetter_temp_df['stn']=='STG'].copy(deep=True)
stg_temp_df["timer"] = stg_temp_df["time"].apply(splitStringDate)
stg_temp_df["date"] = stg_temp_df["time"].apply(splitStringDate4)

stg_temp_df =  stg_temp_df[(stg_temp_df['timer'].dt.hour >= 8) & (stg_temp_df['timer'].dt.hour <=18)]

stg_temp_df =  stg_temp_df[wetter_temp_df['tre200h0']!='-'].copy(deep=True)
stg_temp_df["tre200h0"] = pd.to_numeric(stg_temp_df["tre200h0"])

temp_mean = stg_temp_df.groupby(['date'],as_index=True).agg({'tre200h0': 'mean'})


print('Veloo')

df_velos = pd.read_csv('velozahlungen-stadt-stgallen.csv',sep=";")
df_velos = df_velos.sort_values(by=['Datum'])
df_velos['Time'] = df_velos["Datum"].apply(splitStringDate2)
#df_velos['Date'] = df_velos["Datum"].apply(splitStringDate3)
df_velos['Date'] = df_velos["Time"].dt.date
#df_velos =  df_velos[df_velos['Bezeichnung']=='Museumstrasse']
strassen = df_velos.groupby(['Bezeichnung'],as_index=True).agg({'Anzahl Velos': 'sum'})
# df_velos =  df_velos[df_velos['Bezeichnung']=='Rosenbergstrasse Veloweg']
# df_velos

print('Veloo fertig')


superwetter = superwetter.set_index('date')
df_daily_Wetter = df_daily_Wetter.set_index('date')


superwetter =  superwetter[superwetter['stn']=='STG']
ultra_df = pd.merge(superwetter, df_daily_Wetter, left_index=True, right_index=True)
ultra_df = pd.merge(ultra_df, temp_mean, left_index=True, right_index=True)
# = pd.merge(super_df, maxi_df, left_index=True, right_index=True)

print(superwetter.head())
print(df_daily_Wetter.head())


ultra_df['date'] = ultra_df.index
#ultra_mega_df = ultra_df[['Anzahl Velos', 'date', 'Arbeitstag', 'tre200h0', 'rre024i0', 'sremaxdv', 'hns000j0', 'ure200d0', 'htoautj0']]

mapping2 = {'-': 0}
ultra_df = ultra_df.replace({'rre024i0': mapping2, 'rre024i0': mapping2,
                                       'sremaxdv': mapping2, 'hns000j0': mapping2,
                                      'ure200d0': mapping2, 'htoautj0': mapping2})
ultra_df['rre024i0'] = pd.to_numeric(ultra_df['rre024i0'])
ultra_df['sremaxdv'] = pd.to_numeric(ultra_df['sremaxdv'])
ultra_df['hns000j0'] = pd.to_numeric(ultra_df['hns000j0'])
ultra_df['htoautj0'] = pd.to_numeric(ultra_df['htoautj0'])

n_map = {'Anzahl Velos': 'velos', 'tre200h0': 'temp', 'rre024i0': 'regen',
         'sremaxdv': 'sonne', 'hns000j0': 'neuschnee', 'ure200d0': 'feuchtigkeit', 'htoautj0': 'schnee'}
ultra_df = ultra_df.rename(columns=n_map)
models = {}
errors = {}
for index, row in strassen.iterrows():
    print('iter')
    strasse = index
    df_velos_f =  df_velos[df_velos['Bezeichnung']==strasse]

    df2 = df_velos_f.groupby(['Date', 'Arbeitstag'],as_index=False).agg({'Anzahl Velos': 'sum' })
    df2 = df2.set_index('Date')
    fig, ax = plt.subplots(figsize=(10,7))
    ax.plot(df2.index, df2['Anzahl Velos'])
    ax.set_title(strasse)
    ax.set_xlabel('Date')
    ax.set_ylabel('Number of velos')
    super_df = pd.merge(df2,ultra_df , left_index=True, right_index=True)
    ultra_mega_df = super_df[['Anzahl Velos', 'date', 'Arbeitstag', 'temp', 'regen', 'sonne',
                              'neuschnee', 'feuchtigkeit', 'schnee']]
    n_map = {'Anzahl Velos': 'velos', 'tre200h0': 'temp', 'rre024i0': 'regen',
         'sremaxdv': 'sonne', 'hns000j0': 'neuschnee', 'ure200d0': 'feuchtigkeit', 'htoautj0': 'schnee'}
    ultra_mega_df = ultra_mega_df.rename(columns=n_map)
    mapping = {'Wochenende': 1, 'Werktage': 2}
    ultra_mega_df = ultra_mega_df.replace({'Arbeitstag': mapping})


    print(f'======Strasse: {strasse}======')
    model, error = make_regression(ultra_mega_df)
    models[strasse] = model
    errors[strasse] = error