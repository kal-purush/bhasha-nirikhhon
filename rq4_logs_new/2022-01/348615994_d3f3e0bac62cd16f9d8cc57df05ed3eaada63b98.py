#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# +5.000000000s 0 CustomerApp:SendQuery(): [DEBUG] Start-process-Order newRecord_Node0_9

# +5.053679998s 0 CustomerApp:OnData(): [DEBUG] Order-Complete newRecord_Node0_9

# +5.053679998s 0 CustomerApp:OnData(): [DEBUG] fulfill-order newRecord_Node0_9 data-name 00000001

# +16.140871992s 15 DataStore:OnInterest(): [DEBUG] Store-data-complete newRecord_Node0_7

import json

def main():
    # f = open("nccu1115_only_ndn.txt")

    # f = open("nccu1121_5kbuk_half.txt")
    # file = open('nccu_output.json', "w")

    f = open("nccu1121_origin_10kbuk_half.txt")
    file = open('nccu_output_origin.json', "w")

    # f = open("nccu1121_noNDN_10kbuk_offline.txt")
    # file = open('nccu_output_noNDN.json', "w")

    gap = 500
    time = 1000
    output_arr = []
    temp_count = 0

    data = dict()
    for line in f.readlines():

        words = line.split();
        if len(words) < 5:
            continue
        if len(words[0]) < 13 :
            continue
        if len(words[0]) > 17:
            continue
            
        try:
            current_time = float(words[0].strip('s').strip('+'))
        except:
            continue

        if current_time > time:
            output_arr.append(temp_count)
            temp_count = 0
            time = time + gap
        

        if words[4] == 'Kbucket_connect:':
            temp_count += 1
            continue
        
        # if words[4] == 'Kbucket_disconnect:':
        #     temp_count += 1
            
        #     continue
            
            
            
    # print(json.dumps(data,sort_keys=True, indent=4))
    print('process done')
    f.close()

    json.dump(data, file)
    file.close()

    print(output_arr)

if __name__ == '__main__':
    main()