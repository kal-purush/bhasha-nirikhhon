from tools import constant as constant, utils as utils
import os

Count = [[0] * 12 for i in range(12)]


def build_count_list(data):
    del data[0]
    for line in data:
        line_list = line.split(',')
        for i in range(1, 12):
            for j in range(i+1, 13):
                if line_list[i] == line_list[j] and line_list[i] == "T":
                    Count[i-1][j-1] += 1
    return 0


if __name__ == '__main__':
    print('start')
    data = utils.read_file(os.path.join(constant.data_files_path, constant.cust_prod_dat_name))
    build_count_list(data)
    for i in range(12):
        print(Count[i])
        print('\n')
    print("end")