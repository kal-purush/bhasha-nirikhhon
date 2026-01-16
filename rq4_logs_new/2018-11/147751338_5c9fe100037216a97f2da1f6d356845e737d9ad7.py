# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

def checkio(data):
    digit = 0
    upper = 0
    lower = 0
    if len(data) < 10:    #密码的长度是否大于或等于10个字符
        return False
    else:
        for i in range(len(data)):
            if data[i].isdigit():  ##遍历密码中的数字个数，保存在digit中
                digit += 1
            elif data[i].islower(): ##遍历密码中的小写字母个数，保存在digit中
                lower += 1
            elif data[i].isupper(): ##遍历密码中的大写个数，保存在digit中
                upper += 1
        if digit and lower and upper:  ###密码中至少有一个数字、一个大写字母和一个小写字母
            return True
        else:
            return False

if __name__ == '__main__':
    print(checkio('wjsjakgjie'))
    print("程序正常")