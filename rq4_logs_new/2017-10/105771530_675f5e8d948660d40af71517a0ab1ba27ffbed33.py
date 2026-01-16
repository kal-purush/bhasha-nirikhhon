#!/usr/bin/python
import csv
import glob
import os
import errno
from mqtt_performance_tester.analyze_with_dup import computeTime, compute_e2e_latency

def create_cvs(filename, values):

        with open('%s.csv' % filename, 'w') as csvfile:
            fieldnames = ['payload', 'values']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            for payload in values.keys():
                writer.writerow({'payload': payload, 'values': values[payload]})

def compute_all_file(path):

    sims  = dict()
    link_dis = ""
    for f in glob.glob(os.path.join(path, '*.json')):
        parts = f.split("/")
        file_id  = parts[2].replace(".json", "")
        link_dis = parts[1]
        info = file_id.split("_")
        num = int(info[8].replace(".json", ""))
        qos = info[3]
        payload = info[5]
        data = computeTime(f, num, qos)
        min_v, avg_v, max_v, all_data = compute_e2e_latency(data.packets)
        if qos not in sims:
            sims[qos] = dict()
        sims[qos][payload] = all_data

    for qos in sims:
        create_cvs("%s_qos_%s"%(link_dis, qos), sims[qos])


if __name__ == '__main__':


    print ("Please enter the root directory:")
    directory = input("DIR:")
    print ("Reading data in this folder: {0}".format(directory))
    print (os.listdir(directory))
    for dir in os.listdir(directory):
	    compute_all_file("%s/%s"%(directory,dir))