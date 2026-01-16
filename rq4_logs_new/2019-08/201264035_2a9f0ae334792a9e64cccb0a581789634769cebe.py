#!/user/bin/python3
#1--import导入和from……import……
# 在 python 用 import 或者 from...import 来导入相应的模块。
# 将整个模块(somemodule)导入，格式为： import somemodule
# 从某个模块中导入某个函数,格式为： from somemodule import somefunction
# 从某个模块中导入多个函数,格式为： from somemodule import firstfunc, secondfunc, thirdfunc
# 将某个模块中的全部函数导入，格式为： from somemodule import *

import sys
for i in sys.argv:
    print('i,',i)
print('sys.path',sys.path)

from sys import path
print('sys.path',path)


# i, E:/HQ/pythonlearn/pythonlearncode/0-data-type
# sys.path ['E:\\HQ\\pythonlearn\\pythonlearncode', 'E:\\HQ\\pythonlearn\\pythonlearncode', 'D:\\Software\\PyCharm 2019.1\\helpers\\pycharm_display', 'D:\\Anaconda3\\python35.zip', 'D:\\Anaconda3\\DLLs', 'D:\\Anaconda3\\lib', 'D:\\Anaconda3', 'C:\\Users\\DELL\\AppData\\Roaming\\Python\\Python35\\site-packages', 'D:\\Anaconda3\\lib\\site-packages', 'D:\\Anaconda3\\lib\\site-packages\\Sphinx-1.4.6-py3.5.egg', 'D:\\Anaconda3\\lib\\site-packages\\win32', 'D:\\Anaconda3\\lib\\site-packages\\win32\\lib', 'D:\\Anaconda3\\lib\\site-packages\\Pythonwin', 'D:\\Software\\PyCharm 2019.1\\helpers\\pycharm_matplotlib_backend']
# sys.path ['E:\\HQ\\pythonlearn\\pythonlearncode', 'E:\\HQ\\pythonlearn\\pythonlearncode', 'D:\\Software\\PyCharm 2019.1\\helpers\\pycharm_display', 'D:\\Anaconda3\\python35.zip', 'D:\\Anaconda3\\DLLs', 'D:\\Anaconda3\\lib', 'D:\\Anaconda3', 'C:\\Users\\DELL\\AppData\\Roaming\\Python\\Python35\\site-packages', 'D:\\Anaconda3\\lib\\site-packages', 'D:\\Anaconda3\\lib\\site-packages\\Sphinx-1.4.6-py3.5.egg', 'D:\\Anaconda3\\lib\\site-packages\\win32', 'D:\\Anaconda3\\lib\\site-packages\\win32\\lib', 'D:\\Anaconda3\\lib\\site-packages\\Pythonwin', 'D:\\Software\\PyCharm 2019.1\\helpers\\pycharm_matplotlib_backend']


#2--python数据类型
'''
Python3 的六个标准数据类型中：
不可变数据（3个）：Number（数字）、String（字符串）、Tuple（元组()）；
可变数据（3个）：List（列表[]）、Dictionary（字典{}）、Set（集合()）。
其中，Number又包括int长整型 complex复数，bool布尔型，float浮点型。
'''
#2-1 Number数字类型

# 注意：Python的整数没有大小限制，而某些语言的整数根据其存储长度是有大小限制的，
# 例如Java对32位整数的范围限制在-2147483648-2147483647。
# Python的浮点数也没有大小限制，但是超出一定范围就直接表示为inf（无限大）。

a,b,c,d= 4+3j,100,True,5.5
print(type(a),type(b),type(c),type(d))
#<class 'complex'> <class 'int'> <class 'bool'> <class 'float'>

print(2**5)     #乘方  32
print(2/4)      #除法 0.5
print(2//4)     #整除值 0
print(9//2)     #整除值 4
print(-9//2)    #整除值 -5
#python数值除法包含两个运算符：
# （精确除法）/ 返回一个浮点数，即使是两个整数恰好整除，结果也是浮点数：9/3为3.0
# （地板除）//  返回一个整数,向下取接近除数的整数	9//2=4,-9//2=-5
print(13%2)     #取余 1

#2-2 String 字符串-和List[]列表、tuple()元组类型
# 变量[头下标:尾下标]
# Python中的字符串有两种索引方式，从左往右以0开始，从右往左以-1开始
# 加号+是字符串的连接符， 星号*表示复制当前字符串，紧跟的数字为复制的次数。
# Python中的String字符串不能改变，但是list可以改变。
# 元组tuple的元素不能修改。元组写在小括号()里，元素之间用逗号隔开
str1 = 'runoob'
print(str1[2: ])
print(str1[-5])

list1 = ['abcd',908,True,30.2]
tinylist = [12,'runbot']
print(list1+tinylist)    
a = [1,2,3,4,5,6]
a[1:3] = [9,10]     #list列表元素修改
print('a:',a)

tuple1=("apple",123,20.3)
print(tuple1[1:])
tup1 = ()    # 空元组
tup2 = (20,) # 一个元素，需要在元素后添加逗号，防止和数字歧义
print(tup2[0:])

#2-3 Set集合 基本功能是进行成员关系测试和删除重复元素。
# 可以使用大括号{ }或者set()函数创建集合，注意：创建一个空集合必须用 set() 而不是 { }，因为 { } 是用来创建一个空字典。
# 创建格式：
# parame = {value01,value02,...} 或者 set(value)
student = {'Tom',"Jack","Anna","Tom"}
print('student',student)
a=set("abracadabra")
b=set('alacazam')
print('a:',a)
print('b:',b)
print('a - b',a - b)     # a 和 b 的差集
print('a | b',a | b)     # a 和 b 的并集
print('a & b',a & b)     # a 和 b 的交集
print('a ^ b',a ^ b)     # a 和 b 中不同时存在的元素

#2-4 Dictionary 字典
# 列表是有序的对象集合，字典是无序的对象集合。
# 两者之间的区别在于：字典当中的元素是通过键来存取的，而不是通过偏移存取。
# 字典是一种映射类型，字典用{ }标识，它是一个无序的键[key]:值value的集合。
# 键(key)必须使用不可变类型。在同一个字典中，键(key)必须是唯一的。
# 字典是一种映射类型，它的元素是键值对。
# 字典的关键字必须为不可变类型，且不能重复。
# 创建空字典使用 { }

dict1={ }        #创建空字典
dict1['one']="1"
dict1[2]=2
dict1["three"]='third'
print(dict1["one"])      #输出键为"one"的值
print(dict1[2])          #输出键为2的值
print(dict1["three"])    #输出键为"third"的值
print(dict1)

tinydict={"one":"1",2:2,"three":"third"}
print(tinydict["one"])
print(tinydict[2])
print(tinydict)          # 输出完整的字典
print(tinydict.keys())   # 输出所有键
print(tinydict.values()) # 输出所有值

#构造函数dict()可以直接从键值对序列中构建字典
print({x: x**2 for x in (2, 4, 6)}) #幂 - 返回x的2次幂
print({x: x**2 for x in (1, 3, 5)}) #幂 - 返回x的2次幂