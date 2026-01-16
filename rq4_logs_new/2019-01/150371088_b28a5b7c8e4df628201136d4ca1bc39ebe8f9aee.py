#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# __date__ = '2018/7/12 19:01'
# __author__ = 'Colby'
#
# name = "likunhong"
# print("my name is ",name)

# import random
#
# result = random.randint(1,20)
# temp = input("猜数字：")
# guess = int(temp)
#
# while guess != result:
#     temp = input("猜错了，请重新输入：")
#     guess = int(temp)
#     if guess == result:
#         print('猜对了')
#     else:
#         if guess > result:
#             print("大了")
#         else:
#             print("小了")
# print("游戏结束")

# print("你好".encode().decode())
# a=2
# b = 3
# a,b

# name = [1,3,5,2,6,3]
# print(name)
# name1 = "likunhong"
# print(name1)


# number = [1,0,2,3,4,5,6,7,8,'dddd']
# print(number[-2:])

import  copy

# list1 = [1,2,3]
# list2 = [2,3,4]
# list3 = list1.copy()
# list4 = copy.deepcopy(list1)
# list = [1,2,3,2,3,4]
# print(list)
# list.sort(reverse = True)
# print(list)


# name = 'likunhong'
# print(name.center(40,"-"))
#
# print("?".join("我是你吗你是我吗你在干吗"))


mydict ={
    1:2,2:3
}
print
c = dict.fromkeys((1,2,3),"1")
print(c)