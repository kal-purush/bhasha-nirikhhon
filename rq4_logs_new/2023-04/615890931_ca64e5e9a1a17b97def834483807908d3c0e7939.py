import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
import sys

if __name__ == '__main__':
    #https://csr.lanl.gov/data/2017/
    file = sys.argv[1]
    #file = 'netflow_day-03'
    df = pd.read_csv(file + '.csv', header=None)
    df.columns = ['Time','Duration','SrcDevice','DstDevice','Protocol','SrcPort','DstPort','SrcPackets','DstPackets','SrcBytes','DstBytes']

    #Byte数が大きいので、10^6 -> Mbyteにする
    df['SrcPackets'] = df['SrcPackets'] / 10**6
    df['DstPackets'] = df['DstPackets'] / 10**6
    df['SrcBytes'] = df['SrcBytes'] / 10**6
    df['DstBytes'] = df['DstBytes'] / 10**6
    #df.describe()


    SrcDevice_list = df['SrcDevice'].unique().tolist()
    #print(len(SrcDevice_list))
    SrcDevice_df = pd.DataFrame({'SrcDevice': SrcDevice_list})
    SrcDevice_df.to_csv(file + '_srcdevice.csv', index=False)


    DstDevice_list = df['DstDevice'].unique().tolist()
    #print(len(DstDevice_list))
    DstDevice_df = pd.DataFrame({'DstDevice': DstDevice_list})
    DstDevice_df.to_csv(file + '_dstdevice.csv', index=False)


    #和集合を求める
    SrcDevice_set = set(SrcDevice_list)
    DstDevice_set = set(DstDevice_list)
    Device_set = SrcDevice_set | DstDevice_set
    Device_list = list(Device_set)
    Device_df = pd.DataFrame({'Device': Device_list})
    Device_df.to_csv(file + '_device.csv', index=False)


    #PC間の通信回数を抽出
    device_weight = []
    from_device = []
    to_device = []
    from_packets = []
    to_packets = []
    from_bytes = []
    to_bytes = []
    duration = []

    start = time.time()
    index = 1
    for src, sub_df in df.groupby('SrcDevice'):
        for dst, subsub_df in sub_df.groupby('DstDevice'):
            from_device.append(src)
            to_device.append(dst)
            cnt = len(subsub_df)
            device_weight.append(cnt)
            from_packets.append(subsub_df['SrcPackets'].sum())
            to_packets.append(subsub_df['DstPackets'].sum())
            from_bytes.append(subsub_df['SrcBytes'].sum())
            to_bytes.append(subsub_df['DstBytes'].sum())
            duration.append(subsub_df['Duration'].sum())
            
        print('{0} %完了'.format(index / len(SrcDevice_list) * 100))
        index += 1
    #    if index > 30:
    #        break
        
    df_sum = pd.DataFrame({'from': from_device, 'to': to_device, 'count' : device_weight, 'duration': duration, 'from_packets': from_packets, 'to_packets': to_packets, 'from_bytes': from_bytes, 'to_bytes': to_bytes})
    df_sum.to_csv(file + '_sum.csv')
    elapsed_time = time.time() - start
    print ("calclation_time:{0}".format(elapsed_time) + "[sec]")
    #df_sum