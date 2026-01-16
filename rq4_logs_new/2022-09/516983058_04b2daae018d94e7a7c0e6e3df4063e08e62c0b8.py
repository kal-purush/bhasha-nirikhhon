import os
import shutil


def getDirAllInNames(path):
    all_file_name_list = os.listdir(path)
    in_name_list = []
    for file_name in all_file_name_list:
        if ".in" in file_name:
            in_name_list.append(file_name)
    return in_name_list


if __name__ == '__main__':
    all_in_name_list = getDirAllInNames(".")
    all_in_name_list.sort()
    for in_name_list in all_in_name_list:
        num = in_name_list.split(".")[0]
        shellCmd = "g++ -std=c++14 solve.cpp -o GenerateData && ./GenerateData < " + in_name_list + " > " + num + ".out"
        val = os.system(shellCmd)
        print(shellCmd)
    print(all_in_name_list)