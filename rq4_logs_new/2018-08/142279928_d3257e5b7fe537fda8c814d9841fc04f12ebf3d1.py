'''
合并统计每个月的新增开发者
格式：年份.csv
1 2 3 4 5 6 7 8 9 10 11 12
新增代码贡献者数目

当月代码贡献者数量
'''

import json
import os
import csv

def merge(folder):
    cDir = "public"
    allUsers = {}
    strMonths = []

    for i in range(1,10):
        strMonths.append("0" + str(i))
    for i in range(10,13):
        strMonths.append(str(i))

    result = {}

    for year in range(2008,2019):
        newUsers = [0 for x in range(0,12)]
        everyMonthUsers = [0 for x in range(0,12)]
        result[str(year)] = {}

        for month in range(0,12):
            file = cDir + "/" + folder + "/" + str(year) + '-' + strMonths[month] + '-commitsUser.json'
            with open(file,'r') as f:
                data = json.loads(f.read())
                print(data)
            for item in data:
                if( item not in allUsers):
                    allUsers[item] = data[item]
                    newUsers[month] += 1
                everyMonthUsers[month] += 1

        result[str(year)]["newUsers"] = newUsers
        result[str(year)]["everyUsers"] = everyMonthUsers

    with open( cDir + "/" + folder  + '/analysisUsers.json','w',newline="") as f:
            json.dump(result,f)

merge('javascript')