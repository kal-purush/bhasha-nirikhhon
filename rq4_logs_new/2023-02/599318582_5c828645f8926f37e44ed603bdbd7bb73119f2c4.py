import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import os
import string

index2 = (['20121024','20121027','20121030','20130617','20130716','20130719','20130723','20130927','20140416','20140418','20140424','20140708','20140711','20140715','20141014','20141017','20141021','20150416','20150420','20150807','20150811','20150814'])
index1 = (['2012-10-24','2012-10-27','2012-10-30','2013-06-17','2013-07-16','2013-07-19','2013-07-23','2013-09-27','2014-04-16','2014-04-18','2014-04-24','2014-07-08','2014-07-11','2014-07-15','2014-10-14','2014-10-17','2014-10-21','2015-04-16','2015-04-20','2015-08-07','2015-08-11','2015-08-14'])
alphabet_list = list(string.ascii_letters)
k = -3
home ='/mnt/c/Users/asanchez2415/PycharmProjects/Data_Cleaner3'
if k == 2:
    name = 'm_processed_.csv'
    res = '400'
    intial_data = pd.read_csv(res+name, dtype=str)
    x = pd.DataFrame(columns=['PageName','Clay','Sand','Silt ','Elevation','Slope','Ascept','NDVI','Smerge','Air','Date','Lai','Albedo','Water'])
    for j in range(1, 23):
        i = str(j)
        lai_data = pd.read_csv(home+'/LAI/LAI_'+i+'_'+res+'.csv', usecols=['PageName', 'MEAN'], dtype=str)
        lai_data.columns = ['PageName', 'Lai']
        lai_data['Date'] = index2[j-1]
        albedo_data = pd.read_csv(home+'/Albedo/Alb_'+i+'_'+res+'.csv', usecols=['PageName', 'MEAN'], dtype=str)
        albedo_data.columns = ['PageName', 'Albedo']
        albedo_data['Date'] = index2[j-1]
        if j <= 3:
            print(2012)
            water_data = pd.read_csv(home+'/water_mask/water_2012_'+res+'.csv', usecols=['PageName', 'MEAN'])
        elif j <= 8:
            print(2013)
            water_data = pd.read_csv(home + '/water_mask/water_2013_' + res + '.csv', usecols=['PageName', 'MEAN'])
        elif j <= 17:
            print(2014)
            water_data = pd.read_csv(home + '/water_mask/water_2014_' + res + '.csv', usecols=['PageName', 'MEAN'])
        elif j <= 22:
            print(2015)
            water_data = pd.read_csv(home + '/water_mask/water_2015_' + res + '.csv', usecols=['PageName', 'MEAN'])
        water_data.columns = ['PageName', 'Water']
        water_data['Date'] = index2[j - 1]
        fin = pd.merge(intial_data, lai_data, on=['PageName','Date'])
        fin1 = pd.merge(fin, albedo_data, on=['PageName','Date'])
        fin2 = pd.merge(fin1, water_data, on=['PageName','Date'])
        x = pd.concat([x, fin2])
    fin3 = x[x['Water'] <= 0.25]
    fin3.to_csv('A2_' + res + name, index=False)
if k == 4:
    data = pd.read_csv('A2_1000m_processed_.csv', dtype=str)
    data['LST_DI'] = None
    data['LST_D_'] = None
    data['LST_N_'] = None
    data.to_csv('B_1000m_processed_.csv', index=False)
    print(data.columns)
if k == 5:
    dates = ['20121030','20140418','20140711','20141014','20150811']
    x = pd.DataFrame(
        columns=['PageName', 'Clay', 'Sand', 'Silt ', 'Elevation', 'Slope', 'Ascept',
                 'NDVI', 'Smerge', 'Air', 'Date', 'Lai', 'Albedo', 'Water', 'LST_DI',
                 'LST_D_', 'LST_N_'])
    y = pd.DataFrame(
        columns=['PageName', 'Clay', 'Sand', 'Silt ', 'Elevation', 'Slope', 'Ascept',
                 'NDVI', 'Smerge', 'Air', 'Date', 'Lai', 'Albedo', 'Water', 'LST_DI',
                 'LST_D_', 'LST_N_'])
    h = pd.DataFrame(columns=['PageName', 'LST_DI', 'LST_D_', 'LST_N_', 'Date'])
    for j in range(0, 5):
        print(dates[j])
        date = dates[j]
        dir_path = home + '/LST/1000m/' + date
        data = pd.read_csv('A2_1000m_processed_.csv', dtype=str)
        for filename in os.listdir(dir_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(dir_path, filename)
                print(filename)
                lst_a = pd.read_csv(file_path, dtype=str)
                lst_a['Date'] = date
                h['Date'] = date
                h['PageName'] = lst_a['PageName']
                h['LST_' + filename[0:2]] = lst_a['MEAN']
                # pop = lst_a[['MEAN', 'PageName','Date']]
                # pop.columns = ['LST_'+filename[0:2],'PageName','Date']
                # data_f = pd.merge(pop,on='PageName')
                # data_f = pd.merge(data, pop, on=['PageName','Date'])
                print(h)
                print('LST_' + filename[0:2])
        x = pd.merge(data, h, on=['PageName', 'Date'])
        y = pd.concat([y,x])
    y.to_csv('B2_1000m_processed_.csv', index=False)
if k == 3:
    input_dir = home+'/Data'
    output_dir = home
    data = []
    for file in os.listdir(input_dir):
        if file.endswith(".csv"):
            data.append(os.path.join(input_dir, file))
    for data in data:
        print(data)
        output_file = os.path.join(output_dir, os.path.basename(data))
        output_file_test = os.path.splitext(output_file)[0] + "_test.csv"
        output_file_train = os.path.splitext(output_file)[0] + "_train.csv"

        i = pd.read_csv(data)
        #j = i.corr()
        df_train, df_test = train_test_split(i, test_size=0.30, random_state=42)
        df_train.to_csv(output_file_train, index=False)
        df_test.to_csv(output_file_test, index=False)
        #j.to_csv(output_file)

    print("Process complete")
if k == -2:
    data = pd.read_csv('SS_30m_data.csv')
    data1 = data[(data['LAI'] <= 249) & (data['Albedo'] != 32767) & (data['Ascept'] > 0)]
    data.to_csv('SS_30m_data_p.csv', index=False)
if k == -3:
    data = pd.read_csv('SS_30m_data_p.csv')
    #df_train, df_test = train_test_split(data, test_size=0.30, random_state=42) #whole
    # df_test = data[(data['PageName'] == 'A1') | (data['PageName'] == 'A2') | (data['PageName'] == 'B2')|(data['PageName'] == 'C2')|(data['PageName'] == 'A3')|(data['PageName'] == 'B3')|(data['PageName'] == 'A4')
    #                | (data['PageName'] == 'B4') | (data['PageName'] == 'A5') | (data['PageName'] == 'B5')]#100m
    # df_train = data[(data['PageName'] != 'A1') & (data['PageName'] != 'A2') & (data['PageName'] != 'B2') & (
    #             data['PageName'] != 'C2') & (data['PageName'] != 'A3') & (data['PageName'] != 'B3') & (data['PageName'] != 'A4') &
    #             (data['PageName'] != 'B4') & (data['PageName'] != 'A5') & (data['PageName'] != 'B5')] #100m
    df_test = data[(data['PageName'] == 'A1') | (data['PageName'] == 'B1') | (data['PageName'] == 'C1') | (
                data['PageName'] == 'B6') | (data['PageName'] == 'D6') | (data['PageName'] == 'H6') | (data['PageName'] == 'B9')
                   | (data['PageName'] == 'D9') | (data['PageName'] == 'F9') | (data['PageName'] == 'B12') | (data['PageName'] == 'F12') | (data['PageName'] == 'B15')
                   | (data['PageName'] == 'F15') | (data['PageName'] == 'D17')] #30m
    df_train = data[(data['PageName'] != 'A1') & (data['PageName'] != 'B1') & (data['PageName'] != 'C1') & (
            data['PageName'] != 'B6') & (data['PageName'] != 'D6') & (data['PageName'] != 'H6') & (data['PageName'] != 'B9')
                   & (data['PageName'] != 'D9') & (data['PageName'] != 'F9') & (data['PageName'] != 'B12') & (data['PageName'] != 'F12') & (data['PageName'] != 'B15')
                   & (data['PageName'] != 'F15') & (data['PageName'] != 'D17')] #30m

    df_train.to_csv('SS_30m_data_train.csv', index=False)
    df_test.to_csv('SS_30m_data_test.csv', index=False)
if k == -1:
    input_dir = "/mnt/d/SoilScape"
    whole = []
    thirty_m = []
    hundred_m = []
    for f in os.listdir(input_dir):
        if f.endswith("whole.csv"):
            whole.append(f)
        if f.endswith("30m.csv"):
            thirty_m.append(f)
        if f.endswith("100m.csv"):
            hundred_m.append(f)
    static_whole = pd.DataFrame(index=range(1))
    for w in whole:
        temp = pd.read_csv(os.path.join(input_dir,w))
        static_whole[w.split("_", 1)[0]] = temp['MEAN']
        static_whole.to_csv("SS_Static_whole.csv",index=False)
    static_thirty = pd.DataFrame(index=range(190))
    for t in thirty_m:
        temp = pd.read_csv(os.path.join(input_dir,t))
        static_thirty[t.split("_", 1)[0]] = temp['MEAN']
        static_thirty.to_csv("SS_Static_30m.csv",index=False)
    static_hundred = pd.DataFrame(index=range(18))
    for h in hundred_m:
        temp = pd.read_csv(os.path.join(input_dir,h))
        static_hundred[h.split("_", 1)[0]] = temp['MEAN']
        static_hundred.to_csv("SS_Static_100m.csv",index=False)

if k == 1:
    smerge = pd.read_csv('/mnt/d/SoilScape/New folder/smerge_soilscape.csv')
    static_whole = pd.read_csv("SS_Static_whole.csv")
    static_hundred = pd.read_csv("SS_Static_100m.csv")
    static_thirty = pd.read_csv("SS_Static_30m.csv")
    input_dir = "/mnt/d/SoilScape/CSVs"
    whole = []
    thirty_m = []
    hundred_m = []
    for f in os.listdir(input_dir):
        if f.endswith("whole.csv"):
            whole.append(f)
        if f.endswith("30m.csv"):
            thirty_m.append(f)
        if f.endswith("100m.csv"):
            hundred_m.append(f)
    print(thirty_m)
    print(123456789)
    alb_whole = []
    alb_thirty_m = []
    alb_hundred_m = []
    lai_whole = []
    lai_thirty_m = []
    lai_hundred_m = []
    ndvi_whole = []
    ndvi_thirty_m = []
    ndvi_hundred_m = []
    for w in whole:
        if w.startswith("Alb"):
            alb_whole.append(os.path.join(input_dir,w))
        if w.startswith("LAI"):
            lai_whole.append(os.path.join(input_dir,w))
        if w.startswith("NDVI"):
            ndvi_whole.append(os.path.join(input_dir,w))
    for t in thirty_m:
        if t.startswith("Alb"):
            alb_thirty_m.append(os.path.join(input_dir,t))
        if t.startswith("LAI"):
            lai_thirty_m.append(os.path.join(input_dir,t))
        if t.startswith("NDVI"):
            ndvi_thirty_m.append(os.path.join(input_dir,t))
    for h in hundred_m:
        if h.startswith('A'):
            alb_hundred_m.append(os.path.join(input_dir,h))
        if h.startswith('L'):
            print(h)
            lai_hundred_m.append(os.path.join(input_dir,h))
            print(lai_hundred_m)
        if h.startswith('N'):
            ndvi_hundred_m.append(os.path.join(input_dir,h))
    print(lai_hundred_m)
    f_data = pd.DataFrame(columns=['SMERGE', 'Date', 'PageName', 'LAI', 'Albedo', 'NDVI', 'Clay', 'Sand', 'Silt ', 'Slope', 'Elevation','Ascept'])
    doy = ['4/21/2012', '6/1/2012', '6/8/2012', '6/15/2012', '8/24/2012', '8/31/2012', '9/7/2012', '9/22/2012', '9/29/2012', '10/6/2012', '10/13/2012',
           '10/20/2012', '10/30/2012', '4/7/2013', '4/14/2013', '4/21/2013', '4/28/2013', '5/5/2013', '5/12/2013', '5/19/2013', '5/30/2013', '6/6/2013',
           '6/13/2013', '8/1/2013', '10/6/2013', '10/13/2013', '10/20/2013', '10/27/2014', '4/1/2015', '4/8/2015', '4/15/2015', '4/22/2015', '4/29/2015',
           '5/6/2015', '5/13/2015', '5/20/2015', '5/27/2015', '6/3/2015', '6/10/2015', '6/17/2015', '6/24/2015', '7/1/2015', '7/8/2015', '7/15/2015',
           '7/22/2015', '7/29/2015', '8/5/2015', '8/12/2015', '8/19/2015', '8/26/2015', '9/2/2015', '9/9/2015', '9/16/2015', '9/23/2015', '9/30/2015',
           '10/7/2015', '10/14/2015', '10/21/2015', '10/28/2015']
    r = 1
    current = pd.DataFrame( index=range(r))
    for w in range(0,59):
        #print(len(alb_whole))
        #print(lai_whole)
        print(w)
        static = pd.read_csv("SS_Static_whole.csv")
        sm = pd.DataFrame(smerge.iloc[w]['SMERGE'], columns=['SMERGE'], index=range(r))
        current['SMERGE'] = sm['SMERGE']
        current["Date"] = pd.DataFrame(doy[w], columns=['Date'], index=range(r))
        current["PageName"] = pd.read_csv(alb_whole[w], usecols=["Page_Name"])
        current["LAI"] = pd.read_csv(lai_whole[w], usecols=["MEAN"])
        current["Albedo"] = pd.read_csv(alb_whole[w], usecols=["MEAN"])
        current["NDVI"] = pd.read_csv(ndvi_whole[w], usecols=['MEAN'])
        current[['Clay' ,'Sand' ,'Silt ','Slope' ,'Elevation' ,'Ascept']] = static[['Clay' ,'Sand' ,'Silt','Slope' ,'Elevation' ,'Ascept']]
        f_data = pd.concat([f_data, current])
        if w == 58:
            f_data.to_csv('SS_whole_data.csv', index=False)
    r = 18
    current = pd.DataFrame(index=range(r))
    f_data = pd.DataFrame(columns=['SMERGE', 'Date', 'PageName', 'LAI', 'Albedo', 'NDVI', 'Clay', 'Sand', 'Silt ', 'Slope', 'Elevation', 'Ascept'])
    for w in range(0,59):
        #print(w)
        #print(len(alb_hundred_m))
        static = pd.read_csv("SS_Static_100m.csv")
        sm = pd.DataFrame(smerge.iloc[w]['SMERGE'], columns=['SMERGE'], index=range(r))
        current['SMERGE'] = sm['SMERGE']
        current["Date"] = pd.DataFrame(doy[w], columns=['Date'], index=range(r))
        current["PageName"] = pd.read_csv(alb_hundred_m[w], usecols=["PageName"])
        current["LAI"] = pd.read_csv(lai_hundred_m[w], usecols=["MEAN"])
        current["Albedo"] = pd.read_csv(alb_hundred_m[w], usecols=["MEAN"])
        current["NDVI"] = pd.read_csv(ndvi_hundred_m[w], usecols=['MEAN'])
        current[['Clay' ,'Sand' ,'Silt ','Slope' ,'Elevation' ,'Ascept']] = static[['Clay' ,'Sand' ,'Silt','Slope' ,'Elevation' ,'Ascept']]
        f_data = pd.concat([f_data, current])
        if w == 58:
            f_data.to_csv('SS_100m_data.csv', index=False)
    r = 190
    current = pd.DataFrame(index=range(r))
    f_data = pd.DataFrame(columns=['SMERGE', 'Date', 'PageName', 'LAI', 'Albedo', 'NDVI', 'Clay', 'Sand', 'Silt ', 'Slope', 'Elevation', 'Ascept'])
    for w in range(0,59):
        #print(w)
        #print(len(alb_thirty_m))
        static = pd.read_csv("SS_Static_30m.csv")
        sm = pd.DataFrame(smerge.iloc[w]['SMERGE'], columns=['SMERGE'], index=range(r))
        current['SMERGE'] = sm['SMERGE']
        current["Date"] = pd.DataFrame(doy[w], columns=['Date'], index=range(r))
        current["PageName"] = pd.read_csv(alb_thirty_m[w], usecols=["PageName"])
        current["LAI"] = pd.read_csv(lai_thirty_m[w], usecols=["MEAN"])
        current["Albedo"] = pd.read_csv(alb_thirty_m[w], usecols=["MEAN"])
        current["NDVI"] = pd.read_csv(ndvi_thirty_m[w], usecols=['MEAN'])
        current[['Clay' ,'Sand' ,'Silt ','Slope' ,'Elevation' ,'Ascept']] = static[['Clay' ,'Sand' ,'Silt','Slope' ,'Elevation' ,'Ascept']]
        f_data = pd.concat([f_data,current])
        if w == 58:
            f_data.to_csv('SS_30m_data.csv', index=False)