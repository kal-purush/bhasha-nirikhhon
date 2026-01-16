__author__ = 'ziyi'
#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
print(os.name)
#输出操作系统类型，如果是posix，说明系统是Linux、Unix或Mac OS X，如果是nt，就是Windows。
print(os.environ)
#输出操作系统中定义的全部环境变量
print(os.environ.get('PATH'))
#要获取某个环境变量的值，可以调用
os.environ.get('key')

print(os.path.abspath('.'))
#查看当前目录的绝对路径
print(os.getcwd())
# os.mkdir('D:\\vernacular\\create_dir')

# 删除非空的文件目录
# import shutil
# shutil.rmtree('module_mana')

import re
import os
import time
# str.split(string)分割字符串
# 改变目录下指定文件夹的文件名
# def change_name(path):
#     global i
#     if not os.path.isdir(path) and not os.path.isfile(path):
#         return False
#     if os.path.isfile(path):
#         file_path = os.path.split(path)  # 对字符串进行操作，如果分割出目录与文件
#         lists = file_path[1].split('.')  # 分割出文件与文件扩展名
#         file_ext = lists[-1]  # 取出后缀名(列表切片操作)
#         img_ext = ['jpeg', 'psd', 'png', 'jpg']
#         if file_ext in img_ext:
#             os.rename(path, '{}/{}_pc.{}'.format(file_path[0], lists[0],file_ext))
#             i += 1
#     elif os.path.isdir(path):
#         for x in os.listdir(path):
#             change_name(os.path.join(path, x))  # os.path.join()在路径处理上很有用
#
# img_dir = 'D:\\vernacular\\pictures'
# start = time.time()
# i = 0
# change_name(img_dir)
# c = time.time() - start
# print('程序运行耗时:{}'.format(c))
# print('总共处理了 {} 张图片'.format(i))

import os, sys

# 列出目录
# print ("目录为: %s" %os.listdir(os.getcwd()))
# 移除
# os.remove("test_del.py")
# 移除后列出目录
# print ("移除后 : %s" %os.listdir(os.getcwd()))

# 修改文件名称
os.rename("pictures","photo")
os.rename("photo","pictures")
### 会弹出对应的文件夹，可以直接查看文件夹的内容
os.startfile('D:\\vernacular')
print(os)

print(os.path.basename('D:\\vernacular\\pictures'))
print(os.path.dirname('D:\\vernacular\\pictures'))
print(os.path.getatime('D:\\vernacular\\pictures'))
print(os.path.getctime('D:\\vernacular\\pictures'))
print(os.path.getmtime('D:\\vernacular\\pictures'))
print(os.path.getsize('D:\\vernacular\\pictures'))