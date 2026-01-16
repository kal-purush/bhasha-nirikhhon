# coding: utf-8

import re
import pandas as pd

rx_dict = {
    'correct_line': re.compile(r'(?P<correct_line>\| \d+ and \d+ \|)'),
}

values = [0.6552809,
        0.11384258,
        0.30948549,
        0.40668816,
        0.118226675,
        0.161507611,
        0.378394948,
        0.638163870,
        1.551524552,
        2.599507030,
        5.104584863,
        10.291956324,
        20.655504277,
        42.830743142,
        85.605795927,
        172.634450025,
        341.799725340,
        694.92358588,
        1397.822311138,
        2810.677514693]

list_goals = []

def _parse_line(line):

    for key, rx in rx_dict.items():
        match = rx.search(line)
        if match:
            return key, match
    # if there are no matches
    return None, None


def parse_file(filepath,filepathToWrite):
    if filepath == "buf_4096.txt":
        f = open(filepathToWrite,'w')
    else:
        f = open(filepathToWrite,'a+')
    i = 0
    data = []  # create an empty list to collect the data
    # open the file and read through it line by line
    with open(filepath, 'r') as file_object:
        line = file_object.readline()
        while line:
            # at each line check for a match with a regex
            key, match = _parse_line(line)

            # extract school name
            if key == 'correct_line':
                temp = match.group('correct_line');
                temp = temp.replace("| ","")
                temp = temp.replace(" |","")
                temp = temp.replace(" and ",".")
                #print("Line : " + temp + "\n")
                if i < len(values):
                    print(str(i) + "\n")
                    print("Ratio : " + str(values[i]/float(temp)) + "\n")
                    f.write(str(values[i]/float(temp)) + ";")
                    i = i+1



            line = file_object.readline()
        f.write("\n")
        f.close()
    return data


print('parsing start : \n')
files = ["buf_4096.txt","buf_8192.txt","buf_16384.txt","buf_32768.txt","buf_65536.txt","buf_131072.txt","buf_262144.txt","buf_524288.txt","buf_1048576.txt"]
for x in files :
    parse_file(x,"test_parser.txt")
print('\nparsing done!\n')