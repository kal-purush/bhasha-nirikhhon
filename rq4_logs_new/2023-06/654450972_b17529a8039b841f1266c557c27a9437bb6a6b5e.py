import os
import numpy as np
import argparse
#-*- coding: utf-8 -*-

def read_result(file_name, seperate):
    with open(file_name, "r", encoding="utf-8") as file_handle:
        file_lines = file_handle.readlines()
    result_dict = dict()
    for x in range(len(file_lines)):
        line = file_lines[x].strip("\n")
        name = os.path.basename(line.split(" ", 1)[0])
        data = list(map(float, line.split(" ", 1)[1].strip().split()))
        #print(name, data)
        result_dict[name] = data
    return result_dict

def get_cos(data_a, data_b):
    ma = np.linalg.norm(data_a)
    mb = np.linalg.norm(data_b)
    sim = (np.matmul(data_a,data_b))/(ma*mb)
    return sim

def print_cos(data_a, data_b):
    total_sim = 0.0
    data_max_diff = 0.0
    data_max_file = ""
    cos_min = 1.0
    cos_min_file = ""
    cos_max = 0.0
    cos_max_file = ""
    for (key,value) in data_a.items():
        b_data = data_b.get(key)
        sim = get_cos(np.array(value), np.array(b_data))
        if cos_min > sim:
            cos_min = sim
            cos_min_file = key
        if cos_max < sim:
            cos_max = sim
            cos_max_file = key
        diff = np.abs(np.array(value) - np.array(b_data)).max()
        if data_max_diff < diff:
            data_max_diff = diff
            data_max_file = key
        total_sim += sim
    print("average cos: ", total_sim/len(data_a))
    print("all data abs max diff: ", data_max_diff, " filename:", data_max_file)
    print("all file cos min:", cos_min, " filename:", cos_min_file)
    print("all file cos max:", cos_max, " filename:", cos_max_file)

def calac_data_cos(file1, file2,seperate):
    caffe_result = read_result(file1,seperate)
    nnie_result = read_result(file2,seperate)
    print("file1 len: ", len(caffe_result))
    print("file2 len:", len(nnie_result))
    print_cos(caffe_result, nnie_result)



def parse_args():
    parser = argparse.ArgumentParser('AIBEE-INFERENCE-CONVERT')
    parser.add_argument('--f1', required=True)
    parser.add_argument('--f2', required=True)
    parser.add_argument('--sep', required=False, default=' ')
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    cfg_file1 = args.f1
    cfg_file2 = args.f2
    seperate = args.sep
    calac_data_cos(cfg_file1, cfg_file2,seperate)

if __name__ == '__main__':
    main()