#!/usr/bin/env python
import argparse
import os
import time
import pwd
import grp

def list_files():
    f_list = []
    for file in os.listdir('.'):
        if not file.startswith('.'):
            f_list.append(file)
            return '   '.join(f_list)

    print(list_files())

def list_all_files():
    f_list = []
    for file in os.listdir('.'):
        f_list.append(file)
    return '   '.join(f_list)

    print(list_all_files())


def list_files_in_columns():
    for file in os.listdir('.'):
        if not file.startswith('.'):
            file2 = os.path.abspath(os.path.join('.', file))
            if os.path.isdir(file2):
                one = 'd'
            else:
                one = '-'
            two = time.strftime("%m %d %H:%M", time.localtime(os.stat(file2).st_mtime))

            three = pwd.getpwuid(os.stat(file2).st_uid).pw_name

            four = grp.getgrgid(os.stat(file).st_gid).gr_name

            limits = {
                '000': '---',
                '001': '--x',
                '010': '-w-',
                '100': 'r--',
                '011': '-wx',
                '101': 'r-w',
                '110': 'rw-',
                '111': 'rwx',
            }

            def limits_(five):
                limitsnum1 = limits.get(five[0:3], 0)
                limitsnum2 = limits.get(five[3:6], 0)
                limitsnum3 = limits.get(five[6:9], 0)
                return limitsnum1 + limitsnum2 + limitsnum3

            five = str(bin(os.stat(file2).st_mode)[-9:])

            six = one + limits_(five), os.stat(file2).st_nlink, three, four, os.stat(file2).st_size, two, file
            return six

    print(list_files_in_columns())


def list_all_files_in_columns():
    for file in os.listdir('.'):
        file2 = os.path.abspath(os.path.join('.', file))
        if os.path.isdir(file2):
            one = 'd'
        else:
            one = '-'

        two = time.strftime("%m %d %H:%M", time.gmtime(os.stat(file2).st_mtime))

        three = pwd.getpwuid(os.stat(file2).st_uid).pw_name

        four = grp.getgrgid(os.stat(file).st_gid).gr_name

        limits = {
            '000': '---',
            '001': '--x',
            '010': '-w-',
            '100': 'r--',
            '011': '-wx',
            '101': 'r-w',
            '110': 'rw-',
            '111': 'rwx',
        }

        def limits_(five):
            limitsnum1 = limits.get(five[0:3], 0)
            limitsnum2 = limits.get(five[3:6], 0)
            limitsnum3 = limits.get(five[6:9], 0)
            return limitsnum1 + limitsnum2 + limitsnum3

        five = str(bin(os.stat(file2).st_mode)[-9:])
        six = one + limits_(five), os.stat(file2).st_nlink, three, four, os.stat(file2).st_size, two, file
        return six

    print(list_files_in_columns())

parser = argparse.ArgumentParser(prog='rabbit')
parser.add_argument('-a', action='store_true')
parser.add_argument('-l', action='store_true')
args = parser.parse_args()

if args.a == True and args.l ==False:
    list_all_files()
elif args.a == False and args.l == False:
    list_files()
elif args.a == False and args.l == True:
    list_files_in_columns()
else:
    list_all_files_in_columns()