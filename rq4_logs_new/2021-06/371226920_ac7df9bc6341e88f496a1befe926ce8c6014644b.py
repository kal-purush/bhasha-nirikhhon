import pandas as pd
import glob
import numpy as np
from tools.mingamdo_info import mingamdo_parser, make_PD_PV_D0
from tools.mingamdo_preprocessing import mingamdo_design
from tools.dat_info import dat_pd_pv_d0
import scipy.spatial
from collections import Counter
import os
import datetime
import warnings
import traceback
warnings.filterwarnings(action='ignore')


# print('read Design DataFrame')
# f.write('read Design DataFrame')
# df = pd.read_csv('design.csv')
# spacer 2개조합 + 1개 고려
# # + 방법론
# print('read Answer_sheet DataFrame')
# f.write('read Answer_sheet DataFrame')
# answer_sheet = pd.read_csv('answer_sheet_MP.csv')
# - 방법론
# answer_sheet = pd.read_csv('answer_sheet_PM.csv')
# spacer 1개만 고려
# + 방법론
# answer_sheet = pd.read_csv('answer_sheet_MP_1sp.csv')
# - 방법론
# answer_sheet = pd.read_csv('answer_sheet_PM_1sp.csv')


dat_file = glob.glob('C:/Users/sihyun/Desktop/빅데이터 센터/세코닉스/data/DAT/SN084JA_SEKO_0613_DAT/SN084JA_SEKO_TH_210613_N2_13#2 _8.8.8.8.8.8_13.3.16.6.16.3_26(6).28(8).32.32.#5 465.26_315.180.90.180.180.270.dat')

def solution(dat_file):
    # DAT 전처리
    print('Start DAT preprocessing')
    f.write('Start DAT preprocessing\n')
    DAT, measurement_count = dat_pd_pv_d0(dat_file)
    result_count = len(DAT[DAT['Result'] == 1])
    DAT.drop(['Result'], axis=1, inplace=True)
    # 수율 계산
    DAT_yield = result_count / measurement_count * 100
    # PD만 가져오기
    DAT_f = DAT.iloc[:,0:8]
    # 단위 맞추기
    # DAT_f = DAT_f.mul(1000)
    print('get spacer combination')
    f.write('get spacer combination\n')
    spacer_thi_list_v1 = dat_file[0].split(os.sep)[-1].split('_')[-2].split('.')
    spacer_thi_list_v2 = []
    for i in range(len(spacer_thi_list_v1)):
        if '(' in spacer_thi_list_v1[i]:
            temp = spacer_thi_list_v1[i].replace(spacer_thi_list_v1[i], spacer_thi_list_v1[i][:2])
            spacer_thi_list_v2.append(temp)
        else:
            spacer_thi_list_v2.append(spacer_thi_list_v1[i])

    measurement_cal = pd.DataFrame()
    change_val = []
    # 거리계산
    print('calculate distance')
    f.write('calculate distance\n')
    for i in range(len(DAT_f)):
        for j in range(len(answer_sheet)):
            measurement_cal = pd.concat([measurement_cal, pd.DataFrame(DAT_f.iloc[i,:]).T + answer_sheet.iloc[j,:-1]])
        measurement_cal.reset_index(drop=True, inplace=True)
        # 각 measurement 별 0과의 거리
        ary = scipy.spatial.distance.cdist(measurement_cal, pd.DataFrame([0] * 8).T, metric='euclidean')
        result = answer_sheet.loc[answer_sheet.iloc[:, :-1][ary == np.sort(ary.reshape(1, -1))[0][0]].index]
        change_val.extend(list(result['change_value']))
        measurement_cal = pd.DataFrame()
    f.write('solve answer count\n')
    change_list=[]
    r = Counter(change_val)
    if len(r) >= 3:
        for i in range(3):
            change_list.append(sorted(r.items(), key=lambda x: x[1], reverse=True)[i][0])
    elif len(r) == 2:
        for i in range(2):
            change_list.append(sorted(r.items(), key=lambda x: x[1], reverse=True)[i][0])
    else:
        for i in range(1):
            change_list.append(sorted(r.items(), key=lambda x: x[1], reverse=True)[i][0])
    final= pd.DataFrame()
    final['file_name'] = dat_file[0].split(os.sep)[-1]
    final = pd.concat([final, pd.DataFrame(change_list)], axis=1)
    final['file_name'] = dat_file[0].split(os.sep)[-1]
    final.columns = ['file_name', 'change_value']
    # 순위 지정
    if len(final) == 3:
        final.index = [1,2,3]
    elif len(final) == 2:
        final.index = [1,2]
    else:
        final.index = [1]
    a = list(final['change_value'])
    first = []
    second = []
    for i in range(len(final)):
        first.append(a[i][0:6])
        second.append(a[i][6:])
    final['First_change'] = first
    final['Second_change'] = second
    final.drop('change_value', axis=1, inplace=True)
    print('get answer spacer combination')
    f.write('get answer spacer combination\n')
    spacer_change = []
    for i in range(len(final)):
        spacer_thi_list_v3 = spacer_thi_list_v2.copy()
        if 'SP1_+' in final.iloc[i,1]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) + int(final.iloc[i, 1][5])
        elif 'SP1_-' in final.iloc[i,1]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) - int(final.iloc[i, 1][5])
        elif 'SP2_+' in final.iloc[i,1]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) + int(final.iloc[i, 1][5])
        elif 'SP2_-' in final.iloc[i,1]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) - int(final.iloc[i, 1][5])
        elif 'SP3_+' in final.iloc[i, 1]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) + int(final.iloc[i, 1][5])
        elif 'SP3_-' in final.iloc[i, 1]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) - int(final.iloc[i, 1][5])
        elif 'SP4_+' in final.iloc[i, 1]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) + int(final.iloc[i, 1][5])
        elif 'SP4_-' in final.iloc[i, 1]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) - int(final.iloc[i, 1][5])
        elif 'SP5_+' in final.iloc[i, 1]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) + int(final.iloc[i, 1][5])
        elif 'SP5_-' in final.iloc[i, 1]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) - int(final.iloc[i, 1][5])

        if 'SP1_+' in final.iloc[i,2]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) + int(final.iloc[i, 2][5])
        elif 'SP1_-' in final.iloc[i,2]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) - int(final.iloc[i, 2][5])
        elif 'SP2_+' in final.iloc[i,2]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) + int(final.iloc[i, 2][5])
        elif 'SP2_-' in final.iloc[i,2]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) - int(final.iloc[i, 2][5])
        elif 'SP3_+' in final.iloc[i, 2]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) + int(final.iloc[i, 2][5])
        elif 'SP3_-' in final.iloc[i, 2]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) - int(final.iloc[i, 2][5])
        elif 'SP4_+' in final.iloc[i, 2]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) + int(final.iloc[i, 2][5])
        elif 'SP4_-' in final.iloc[i, 2]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) - int(final.iloc[i, 2][5])
        elif 'SP5_+' in final.iloc[i, 2]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) + int(final.iloc[i, 2][5])
        elif 'SP5_-' in final.iloc[i, 2]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) - int(final.iloc[i, 2][5])
        spacer_change.append(str(spacer_thi_list_v3[0])+'.'+str(spacer_thi_list_v3[1])+'.'+str(spacer_thi_list_v3[2])+'.'+str(spacer_thi_list_v3[3])+'.'+str(spacer_thi_list_v3[4]+'.'+str(spacer_thi_list_v3[5])))

    final['spacer'] = spacer_change
    final['yield'] = DAT_yield
    return final

#######################################################################################################################
#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

def solution_1sp(dat_file):
    # DAT 전처리
    print('Start DAT preprocessing')
    f.write('Start DAT preprocessing\n')
    DAT, measurement_count = dat_pd_pv_d0(dat_file)
    result_count = len(DAT[DAT['Result'] == 1])
    DAT.drop(['Result'], axis=1, inplace=True)
    # 수율 계산
    DAT_yield = result_count / measurement_count * 100
    # PD만 가져오기
    DAT_f = DAT.iloc[:,0:8]
    # 단위 맞추기
    # DAT_f = DAT_f.mul(1000)
    print('get spacer combination')
    f.write('get spacer combination\n')
    spacer_thi_list_v1 = dat_file[0].split(os.sep)[-1].split('_')[-2].split('.')
    spacer_thi_list_v2 = []
    for i in range(len(spacer_thi_list_v1)):
        if '(' in spacer_thi_list_v1[i]:
            temp = spacer_thi_list_v1[i].replace(spacer_thi_list_v1[i], spacer_thi_list_v1[i][:2])
            spacer_thi_list_v2.append(temp)
        else:
            spacer_thi_list_v2.append(spacer_thi_list_v1[i])

    measurement_cal = pd.DataFrame()
    change_val = []
    # 거리계산
    print('calculate distance')
    f.write('calculate distance\n')
    for i in range(len(DAT_f)):
        for j in range(len(answer_sheet)):
            measurement_cal = pd.concat([measurement_cal, pd.DataFrame(DAT_f.iloc[i,:]).T + answer_sheet.iloc[j,:-1]])
        measurement_cal.reset_index(drop=True, inplace=True)
        # 각 measurement 별 0과의 거리
        ary = scipy.spatial.distance.cdist(measurement_cal, pd.DataFrame([0] * 8).T, metric='euclidean')
        result = answer_sheet.loc[answer_sheet.iloc[:, :-1][ary == np.sort(ary.reshape(1, -1))[0][0]].index]
        change_val.extend(list(result['change_value']))
        measurement_cal = pd.DataFrame()
    f.write('solve answer count\n')
    change_list=[]
    r = Counter(change_val)
    if len(r) >= 3:
        for i in range(3):
            change_list.append(sorted(r.items(), key=lambda x: x[1], reverse=True)[i][0])
    elif len(r) == 2:
        for i in range(2):
            change_list.append(sorted(r.items(), key=lambda x: x[1], reverse=True)[i][0])
    else:
        for i in range(1):
            change_list.append(sorted(r.items(), key=lambda x: x[1], reverse=True)[i][0])
    final= pd.DataFrame()
    final['file_name'] = dat_file[0].split(os.sep)[-1]
    final = pd.concat([final, pd.DataFrame(change_list)], axis=1)
    final['file_name'] = dat_file[0].split(os.sep)[-1]
    final.columns = ['file_name', 'change_value']
    # 순위 지정
    if len(final) == 3:
        final.index = [1,2,3]
    elif len(final) == 2:
        final.index = [1,2]
    else:
        final.index = [1]
    a = list(final['change_value'])
    first = []
    second = []
    for i in range(len(final)):
        first.append(a[i][0:6])
        second.append(a[i][6:])
    final['First_change'] = first
    final['Second_change'] = second
    final.drop('change_value', axis=1, inplace=True)
    print('get answer spacer combination')
    f.write('get answer spacer combination\n')
    spacer_change = []
    for i in range(len(final)):
        spacer_thi_list_v3 = spacer_thi_list_v2.copy()
        if 'SP1_+' in final.iloc[i,1]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) + int(final.iloc[i, 1][5])
        elif 'SP1_-' in final.iloc[i,1]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) - int(final.iloc[i, 1][5])
        elif 'SP2_+' in final.iloc[i,1]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) + int(final.iloc[i, 1][5])
        elif 'SP2_-' in final.iloc[i,1]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) - int(final.iloc[i, 1][5])
        elif 'SP3_+' in final.iloc[i, 1]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) + int(final.iloc[i, 1][5])
        elif 'SP3_-' in final.iloc[i, 1]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) - int(final.iloc[i, 1][5])
        elif 'SP4_+' in final.iloc[i, 1]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) + int(final.iloc[i, 1][5])
        elif 'SP4_-' in final.iloc[i, 1]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) - int(final.iloc[i, 1][5])
        elif 'SP5_+' in final.iloc[i, 1]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) + int(final.iloc[i, 1][5])
        elif 'SP5_-' in final.iloc[i, 1]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) - int(final.iloc[i, 1][5])

        if 'SP1_+' in final.iloc[i,2]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) + int(final.iloc[i, 2][5])
        elif 'SP1_-' in final.iloc[i,2]:
            spacer_thi_list_v3[0] = int(spacer_thi_list_v3[0]) - int(final.iloc[i, 2][5])
        elif 'SP2_+' in final.iloc[i,2]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) + int(final.iloc[i, 2][5])
        elif 'SP2_-' in final.iloc[i,2]:
            spacer_thi_list_v3[1] = int(spacer_thi_list_v3[1]) - int(final.iloc[i, 2][5])
        elif 'SP3_+' in final.iloc[i, 2]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) + int(final.iloc[i, 2][5])
        elif 'SP3_-' in final.iloc[i, 2]:
            spacer_thi_list_v3[2] = int(spacer_thi_list_v3[2]) - int(final.iloc[i, 2][5])
        elif 'SP4_+' in final.iloc[i, 2]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) + int(final.iloc[i, 2][5])
        elif 'SP4_-' in final.iloc[i, 2]:
            spacer_thi_list_v3[3] = int(spacer_thi_list_v3[3]) - int(final.iloc[i, 2][5])
        elif 'SP5_+' in final.iloc[i, 2]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) + int(final.iloc[i, 2][5])
        elif 'SP5_-' in final.iloc[i, 2]:
            spacer_thi_list_v3[5] = int(spacer_thi_list_v3[5]) - int(final.iloc[i, 2][5])
        spacer_change.append(str(spacer_thi_list_v3[0])+'.'+str(spacer_thi_list_v3[1])+'.'+str(spacer_thi_list_v3[2])+'.'+str(spacer_thi_list_v3[3])+'.'+str(spacer_thi_list_v3[4]+'.'+str(spacer_thi_list_v3[5])))

    final['spacer'] = spacer_change
    final['yield'] = DAT_yield
    return final

#######################################################################################################################
#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

folder_path = input('Input folder name : ')
try:

    f = open('dat\\'+folder_path+'\\'+folder_path+'_log_2sp1sp.log', 'a')

    dir = os.getcwd()
    print(dir)
    f.write('get working directory\n')

    print('read Design DataFrame')
    f.write('read Design DataFrame\n')
    df = pd.read_csv('design.csv')

    # + 방법론
    print('read Answer_sheet DataFrame')
    f.write('read Answer_sheet DataFrame\n')
    answer_sheet = pd.read_csv('answer_sheet_MP.csv')

    dat_paths = glob.glob('dat\\'+folder_path+'\\*.dat')
    print('Folder Name : ', folder_path)
    print('File_num : ', len(dat_paths))

    now = datetime.datetime.now()
    nowDate = now.strftime('%Y-%m-%d')
    nowTime = now.strftime('%H:%M:%S')
    nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
    print(nowDatetime)  # 2021-04-19 12:11:32
    f.write('Date : '+ nowDatetime+'\n')
    solution_df = pd.DataFrame()
    for i in range(len(dat_paths)):
        print('===================================================================')
        dat_file = [dat_paths[i]]
        print('DAT File Name : ', dat_file[0].split(os.sep)[-1])
        f.write('DAT File Name : '+ dat_file[0].split(os.sep)[-1]+'\n')
        solution_df = pd.concat([solution_df, solution(dat_file)])
        print('The extraction of solution is complete')
        f.write('The extraction of solution is complete\n\n')
    print('=================Successfully completed the extraction ========================')
    print('===================================================================')
    f.write('=================Successfully completed the extraction ========================\n')
    f.write('===================================================================\n')


    f.close()
    solution_df.to_csv('dat\\'+folder_path+'\\'+folder_path+'_solution_2sp1sp.csv')
except:
    f.write('================================== ERROR ==========================\n')
    err = traceback.format_exc()
    f.write(err)
    print('================================== ERROR ==========================')
    f.write('===================================================================\n')
    f.close()

#######################################################################################################################
#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

try:

    f = open('dat\\'+folder_path+'\\'+folder_path+'_log_1sp.log', 'a')

    dir = os.getcwd()
    print(dir)
    f.write('get working directory\n')

    print('read Design DataFrame')
    f.write('read Design DataFrame\n')
    df = pd.read_csv('design.csv')

    # + 방법론
    print('read Answer_sheet DataFrame')
    f.write('read Answer_sheet DataFrame\n')
    answer_sheet = pd.read_csv('answer_sheet_MP_1sp.csv')

    dat_paths = glob.glob('dat\\'+folder_path+'\\*.dat')
    print('Folder Name : ', folder_path)
    print('File_num : ', len(dat_paths))

    now = datetime.datetime.now()
    nowDate = now.strftime('%Y-%m-%d')
    nowTime = now.strftime('%H:%M:%S')
    nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
    print(nowDatetime)  # 2021-04-19 12:11:32
    f.write('Date : '+ nowDatetime+'\n')
    solution_df = pd.DataFrame()
    for i in range(len(dat_paths)):
        print('===================================================================')
        dat_file = [dat_paths[i]]
        print('DAT File Name : ', dat_file[0].split(os.sep)[-1])
        f.write('DAT File Name : '+ dat_file[0].split(os.sep)[-1]+'\n')
        solution_df = pd.concat([solution_df, solution_1sp(dat_file)])
        print('The extraction of solution is complete')
        f.write('The extraction of solution is complete\n\n')
    print('=================Successfully completed the extraction ========================')
    print('===================================================================')
    f.write('=================Successfully completed the extraction ========================\n')
    f.write('===================================================================\n')


    f.close()
    solution_df.to_csv('dat\\'+folder_path+'\\'+folder_path+'_solution_1sp.csv')
except:
    f.write('================================== ERROR ==========================\n')
    err = traceback.format_exc()
    f.write(err)
    print('================================== ERROR ==========================')
    f.write('===================================================================\n')
    f.close()