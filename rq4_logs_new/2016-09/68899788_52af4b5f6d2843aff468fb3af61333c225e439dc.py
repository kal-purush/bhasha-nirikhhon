# -*- coding: utf-8 -*-

import re
# 首先建立三个字典，分别存贮总分，考试次数以及平均分，key都是name（名字）
Total_score = {}
Test_times = {}
Avg_score = {}

fr = open("score.txt",'r')
for lines in fr:
    # 这里是当时在网上找的转码格式，在控制台里当时是可以显示中文的，但是后面再放到字典里输出，结果不是中文，
    # 不过进行写的操作时，还是中文，所以并不影响。
    line = lines.decode('gbk').encode('utf-8')
    # 正则表达式找到中文
    namel = re.findall("^[^0-9 ]+",lines)
    name = namel[0]
    # 正则表达式找到数字
    scorel = re.findall("[0-9]+",lines)
    # 转换类型
    score = int(scorel[0])
    # 判断字典里是否有此key，在字典里进行赋值
    if name not in Total_score:
        Total_score[name] = score
        Test_times[name] = 1
    else:
        Total_score[name] += score
        Test_times[name] += 1
    Avg_score[name] = float(Total_score[name]/Test_times[name])
# 在网上找到的一个对字典排序的方法，返回值是个list，元素是tuple
Total_score_sort = sorted(Total_score.iteritems(),key = lambda d:d[1],reverse= True)
# print Total_score,'\n',Test_times,'\n',Avg_score
ff = open('result.txt','w')
for i in Total_score_sort:
    # 新建一个字符串，按名字，总分，考试次数，平均分的顺序进行连接，注意类型转换
    s = i[0] + ' ' + str(Total_score[i[0]]) + ' ' + str(Test_times[i[0]]) + ' ' + str(Avg_score[i[0]]) + '\n'
    ff.write(s)

ff.close()