#!/usr/bin/env python
# -*- coding: utf-8 -*-
import random
import time
import os
import re
import json
import subprocess
import traceback
descriptors = list()
local_path = "/var/run/ceph/"
result_asok = {}
result_ack ={}
NAME_PREFIX = 'osd_'
num = 0
def run_shell(cmd, timeout = 4):
    p = subprocess.Popen(
        args = cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        shell = True
    )
    #must run shell with timeout
    # define deadline
    deadline = time.time() + timeout
    poll_seconds = 0.25
    while time.time() < deadline and p.poll() == None:
        time.sleep(poll_seconds)
    # get result
    ret = p.poll()
    if ret:
        # ret != 0
        return None
    if ret == None:
        # timeout
        try:
            p.terminate()
        except:
            pass
        return None
    res = ''
    while True:
        buff = p.stdout.readline()
        res = res + buff
        if buff == '':
            break;
    return res.rstrip()

def get_all_file(floder_path):
    file_list = []
    if floder_path is None:
        raise Exception("floder_path is None")
    for dirpath, dirnames, filenames in os.walk(floder_path):
        for name in filenames:
            file_list.append(dirpath + '\\' + name)
    return file_list
    
def get_local_osds():
    file_list = get_all_file(local_path)
    osd_list=[]
    for f in file_list:
        m = re.search("ceph-osd\.\d+\.asok",f)
        if m:
            id = f.split('.')[-2]
            osd_list.append(id)
    return osd_list
    
def get_oplatency_journal(name):
    id = name.split('_')[-1]
    try:
        global result_asok
        file_path = local_path + "ceph-osd."+id+".asok"
        result_asok[id] = json.loads(run_shell('ceph --admin-daemon {asok} perf dump'.format(asok=file_path)))
        result = result_asok[id]
        if "filestore" in result:
            if "journal_latency" in result["filestore"]:
                sum_end = result["filestore"]["journal_latency"]["sum"]
                ops_end = result["filestore"]["journal_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/journal_latency'+id):
                    output = open('/dev/shm/journal_latency'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/journal_latency'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n')
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2 :
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/journal_latency'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0
        
def get_oplatency_avgoplat(name):
    id = name.split('_')[-1]
    try:
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "osd" in result:
            if "op_latency" in result["osd"]:
                sum_end = result["osd"]["op_latency"]["sum"]
                ops_end = result["osd"]["op_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/op_latency'+id):
                    output = open('/dev/shm/op_latency'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/op_latency'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n')
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2:
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/op_latency'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0

def get_oplatency_opw(name):
    id = name.split('_')[-1]
    try:
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "op_w_latency" in result["osd"]:
                sum_end = result["osd"]["op_w_latency"]["sum"]
                ops_end = result["osd"]["op_w_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/op_w_latency'+id):
                    output = open('/dev/shm/op_w_latency'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/op_w_latency'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n')
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2:
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/op_w_latency'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0

def get_oplatency_opr(name):
    id = name.split('_')[-1]
    try:
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "op_r_latency" in result["osd"]:
                sum_end = result["osd"]["op_r_latency"]["sum"]
                ops_end = result["osd"]["op_r_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/op_r_latency'+id):
                    output = open('/dev/shm/op_r_latency'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/op_r_latency'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n')
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2:
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/op_r_latency'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0

def get_iops(name):
    id = name.split('_')[-1]
    try:
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "osd" in result:
            if "op_latency" in result["osd"]:
                ops_end = result["osd"]["op_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/iops'+id):
                    output = open('/dev/shm/iops'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/iops'+id,'w')
                #cal 
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 1:
                    return 0
                ops = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    sum_ops = int(ops_end) - int(ops)
                    return sum_ops 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/iops'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0

def get_oplatency_apply(name):
    id = name.split('_')[-1]
    try:
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "filestore" in result:
            if "apply_latency" in result["filestore"]:
                sum_end = result["filestore"]["apply_latency"]["sum"]
                ops_end = result["filestore"]["apply_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/apply_latency'+id):
                    output = open('/dev/shm/apply_latency'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/apply_latency'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n')
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2 :
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/apply_latency'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0
def get_oplatency_commitcycle(name):
    id = name.split('_')[-1]
    try:
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "filestore" in result:
            if "commitcycle_latency" in result["filestore"]:
                sum_end = result["filestore"]["commitcycle_latency"]["sum"]
                ops_end = result["filestore"]["commitcycle_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/commitcycle_latency'+id):
                    output = open('/dev/shm/commitcycle_latency'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/commitcycle_latency'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n')
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2 :
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/commitcycle_latency'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0

def get_queue_transaction(name):
    id = name.split('_')[-1]
    try:
        global result_asok
        if id not in result_asok:
            return 0
        result = result_asok[id]
        if "filestore" in result:
            if "queue_transaction_latency_avg" in result["filestore"]:
                sum_end = result["filestore"]["queue_transaction_latency_avg"]["sum"]
                ops_end = result["filestore"]["queue_transaction_latency_avg"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/queue_transaction_latency_avg'+id):
                    output = open('/dev/shm/queue_transaction_latency_avg'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/queue_transaction_latency_avg'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n')
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2 :
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops
                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/queue_transaction_latency_avg'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0

def get_oplatency_subopw(name):
    id = name.split('_')[-1]
    try:
        if id not in result_asok:
            return 0
        result = result_asok[id]

        if "osd" in result:
            if "subop_w_latency" in result["osd"]:
                sum_end = result["osd"]["subop_w_latency"]["sum"]
                ops_end = result["osd"]["subop_w_latency"]["avgcount"]
                context = []
                if os.path.exists('/dev/shm/subop_w_latency'+id):
                    output = open('/dev/shm/subop_w_latency'+id,'r')
                    while True:
                        line = output.readline()
                        if not line:
                            break
                        context.append(line)
                    output.close()
                output = open('/dev/shm/subop_w_latency'+id,'w')
                #cal 
                output.write(str(sum_end))
                output.write('\n') 
                output.write(str(ops_end))
                output.write('\n')
                output.close()
                if len(context) != 2:
                    return 0
                ops = context[1].strip('\n')
                sum = context[0].strip('\n')
                if int(ops_end) == int(ops):
                    return 0
                else:
                    lat_sum = float(sum_end) - float(sum)
                    lat_ops = int(ops_end) - int(ops)
                    lat = (lat_sum * 1.0)/lat_ops

                    return lat 
            else:
                return 0
        else:
            return 0
    except:
        output = open('/var/log/subop_w_latency'+id, 'w')
        output.write("exception when do : %s" %(traceback.format_exc()))
        output.close()
        return 0


def metric_init(params):
    global descriptors
    osd_list = get_local_osds()
    global result_asok
    funs = {0:get_oplatency_journal,1:get_oplatency_opw,2:get_oplatency_opr,3:get_iops,4:get_oplatency_apply,5:get_oplatency_commitcycle,6:get_queue_transaction,7:get_oplatency_subopw,8:get_oplatency_avgoplat}
    names = {0:"oplatency_journal_",1:"oplatency_opw_",2:"oplatency_opr_",3:"iops_",4:"oplatency_apply_",5:"oplatency_commitcycle_",6:"queue_transaction_",7:"oplatency_subopw_",8:"oplatency_avgoplat_"}
    des = {0:"oplatency_journal",1:"oplatency_opw",2:"oplatency_opr",3:"iops",4:"oplatency_apply",5:"oplatency_commitcycle",6:"queue_transaction",7:"oplatency_subopw",8:"oplatency_avgoplat"}
    values_type = {0:'float',1:'float',2:'float',3:'uint',4:'float',5:'float',6:'float',7:'float',8:'float'}
    format_c = {0:'%f',1:'%f',2:'%f',3:'%u',4:'%f',5:'%f',6:'%f',7:'%f',8:'%f'}
    if osd_list is None:
        return []
    for id in osd_list:
        for i in xrange(0,9):
            d1 = {
                'name': names[i]+NAME_PREFIX + id,
                'call_back': funs[i],
                'time_max': 90,
                'value_type': values_type[i],
                'units': 'C',
                'slope': 'both',
                'format': format_c[i],
                'description': des[i],
                'groups': 'OSD'
            }
            descriptors.append(d1)
    return descriptors
def metric_cleanup():
    pass

#This code is for debugging and unit testing
if __name__ == '__main__':
    metric_init({})

    for d in descriptors:
        v = d['call_back'](d['name'])
        print ('value for %s is '+d['format']) % (d['name'], v)
        