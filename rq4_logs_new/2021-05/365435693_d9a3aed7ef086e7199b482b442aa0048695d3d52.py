from typing import List
import numpy as np
import matplotlib.pyplot as plt


namelist = []
numberlist = []
stu_dict = {}

while 1:
    while 1:
        key = 0
        key1 = 0
        key3 = 0
        print('옵션을 선택하세요 {}'.format('add,show,anz,exit'))
        option = input()
        operation = 0
        if option == 'add':
            operation = 1
            break
        elif option == 'show':
            operation = 2
            break
        elif option == 'anz':
            operation = 3
            break
        elif option == 'exit':
            operation = 4
            break
        else:
            print('옵션을 다시 입력해주세요!!')

    if operation == 1:
        while 1:
            option1 = input("개인(1)으로 받으시겠습니까, 명단(2)으로 받으시겠습니까")
            if option1 == "2":
                f = open("stu_data.txt", 'r')
                while 1:
                    line = f.readline()
                    if not line:
                        break
                    line_list = line.split()
                    stu_dict[line_list[0]] = [line_list[1],
                                              [line_list[2], line_list[3], line_list[4], line_list[5], line_list[6]]]
                    namelist.append(line_list[0])
                    numberlist.append(line_list[1])
                print("학생 명단이 다음과 같이 등록되었습니다.")
                print(stu_dict.keys())
                break
            elif option1 == "1":
                print('학생의 이름을 입력하세요')
                name = input()
                if name in namelist:
                    a = input("동명이인 입니다 입력을 계속 하시겠습니까?")
                    if a == "no":
                        continue
                while 1:
                    print('학생의 출석 번호를 입력하세요.')
                    number = input()
                    if number in numberlist:
                        print("동일한 출석번호가 존재합니다. 다시 입력해주세요.")
                        continue
                    if int(number) > 0:
                        while 1:
                            stu_dict[name] = [number]
                            print('학생의 점수를 입력해주세요')
                            score = input()
                            scorelist = score.split()
                            stu_dict[name].append(scorelist)
                            if int(scorelist[0]) < 0:
                                print('국어 점수를 잘못 입력하셨습니다.')
                                print('점수를 다시 입력해주세요.')
                            if int(scorelist[1]) < 0:
                                print('수학 점수를 잘못 입력하셨습니다.')
                                print('점수를 다시 입력해주세요.')
                            if int(scorelist[2]) < 0:
                                print('사회 점수를 잘못 입력하셨습니다.')
                                print('점수를 다시 입력해주세요.')
                            if int(scorelist[3]) < 0:
                                print('과학 점수를 잘못 입력하셨습니다.')
                                print('점수를 다시 입력해주세요.')
                            if int(scorelist[4]) < 0:
                                print('영어 점수를 잘못 입력하셨습니다.')
                                print('점수를 다시 입력해주세요.')
                            else:
                                print('국어 : {0}\n수학 : {1}\n사회 : {2}\n과학 : {3}\n영어 : {4}'.format(scorelist[0],
                                                                                                scorelist[1],
                                                                                                scorelist[2],
                                                                                                scorelist[3],
                                                                                                scorelist[4]))
                                print('점수가 맞습니까?')
                                answer5 = input()
                                if answer5 == 'yes':
                                    print('입력되었습니다.')
                                    print('학생을 추가 등록 하시겠습니까?')
                                    answer6 = input()
                                    if answer6 == 'yes':
                                        key1 = 1
                                        key = 1
                                        break
                                    if answer6 == 'no':
                                        key3 = 1
                                        key1 = 1
                                        key = 1
                                        break
                            if key == 1:
                                break
                    if key1 == 1:
                        break
                    elif int(number) == 0:
                        print('"{}"학생에 대한 입력을 취소하시겠습니까?'.format(name))
                        answerX = input()
                        if answerX == 'yes':
                            print('입력이 취소되었습니다.')
                            print('학생을 추가 등록 하시겠습니까?')
                            answer6 = input()
                            if answer6 == 'yes':
                                break

                            if answer6 == 'no':
                                key3 = 1
                                key1 = 1
                                key = 1
                                break
                        elif answerX == 'no':
                            key1 = 1
                    elif int(number) < 0:
                        print('정상적인 출석번호가 아닙니다. 제대로 입력해주세요')
                if key3 == 1:
                    break
                namelist.append(name)
                numberlist.append(number)
            else:
                print("옵션을 다시 입력해주세요.")
                continue

    if operation == 2:
        r = 0
        while 1:
            print('이름을 입력하시오')
            name1 = input()
            if not (name1 in namelist):
                print("이런 학생은 없습니다.")
                continue
            print('{}'.format(name1))
            print('출석번호 : {}'.format(stu_dict[name1][0]))
            print(
                '국어 : {0}\n수학 : {1}\n사회 : {2}\n과학 : {3}\n영어 : {4}'.format(stu_dict[name1][1][0], stu_dict[name1][1][1],
                                                                          stu_dict[name1][1][2], stu_dict[name1][1][3],
                                                                          stu_dict[name1][1][4]))
            print("추가로 보시겠습니까? yes or no")
            answer9 = input()
            if answer9 == "yes":
                continue
            elif answer9 == "no":
                break
            else:
                print("그런 명령어는 없습니다. 학생을 다시 입력해주세요")
    if operation == 3:
        keylist = list(stu_dict.keys())
        kuk_list = []
        sulist = []
        salist = []
        sclist = []
        enlist = []
        aeList = []
        while 1:
            print("분석 옵션을 선택하세요. (학생, 전체)")
            choose = input()
            if choose == '학생':
                print('학생에 대한 분석을 시작합니다')
                print('이름을 입력하세요!!')
                name2 = input()
                for i in keylist:
                    kuk_list.append(stu_dict[i][1][0])
                    sulist.append(stu_dict[i][1][1])
                    salist.append(stu_dict[i][1][2])
                    sclist.append(stu_dict[i][1][3])
                    enlist.append(stu_dict[i][1][4])
                    sum = 0
                    for j in range(5):
                        sum = sum + int(stu_dict[i][1][j])
                    aeList.append(sum / 5)
                kuk_list.sort(reverse=True)
                sulist.sort(reverse=True)
                salist.sort(reverse=True)
                sclist.sort(reverse=True)
                enlist.sort(reverse=True)
                aeList.sort(reverse=True)
                print(aeList)
                mean = (float(stu_dict[name2][1][0]) + float(stu_dict[name2][1][1]) + float(stu_dict[name2][1][2])
                        + float(stu_dict[name2][1][3]) + float(stu_dict[name2][1][4])) / 5
                for j in range(39):
                    if stu_dict[name2][1][0] == kuk_list[j]:
                        r = j + 1
                    if stu_dict[name2][1][1] == sulist[j]:
                        r1 = j + 1
                    if stu_dict[name2][1][2] == salist[j]:
                        r2 = j + 1
                    if stu_dict[name2][1][3] == sclist[j]:
                        r3 = j + 1
                    if stu_dict[name2][1][4] == enlist[j]:
                        r4 = j + 1
                    if mean == aeList[j]:
                        r5 = j + 1
                print("홍길동 학생의 성적표 입니다.")
                print('국어점수:{0} {1}등\n수학점수:{2} {3}등\n사회점수:{4} '
                      '{5}등\n과학점수:{6} {7}등\n영어점수:{8} {9}등\n평균:{10} {11}등'.format(stu_dict[name2][1][0], r,
                                                                                 stu_dict[name2][1][1], r1,
                                                                                 stu_dict[name2][1][2], r2,
                                                                                 stu_dict[name2][1][3], r3,
                                                                                 stu_dict[name2][1][4], r4, mean, r5))
                print('분석을 이어서 하시겠습니까?')
                answerxs=input()
                if answerxs=='yes':
                    continue
            elif '전체' == choose:
                kuk_list: List[str] = []
                sulist = []
                salist = []
                sclist = []
                enlist = []
                aeList = []
                print('어떤 과목에 대한 전체 분석을 살펴보시겠습니까?')
                answerlast=input()
                data1=0
                data2=0
                data3=0
                data4=0
                data5=0
                data6=0
                data7=0
                data8=0
                data9=0
                data10=0
                sum = 0
                j=0
                for i in keylist:
                    kuk_list.append(stu_dict[i][1][0])
                    sulist.append(stu_dict[i][1][1])
                    salist.append(stu_dict[i][1][2])
                    sclist.append(stu_dict[i][1][3])
                    enlist.append(stu_dict[i][1][4])
                     #    for j in range(5):
                     # kuk_list.sort(reverse=True)
                     # sulist.sort(reverse=True)
                     # salist.sort(reverse=True)
                     # sclist.sort(reverse=True)
                     # enlist.sort(reverse=True)
                     # aeList.sort(reverse=True)
                if answerlast == '국어':
                    for j in range(39):
                       if int(kuk_list[j])<=10:
                           data1+=1
                       elif int(kuk_list[j])<=20:
                           data2+=1
                       elif int(kuk_list[j])<=30:
                           data3+=1
                       elif int(kuk_list[j])<=40:
                           data4+=1
                       elif int(kuk_list[j])<=50:
                           data5+=1
                       elif int(kuk_list[j])<=60:
                           data6+=1
                       elif int(kuk_list[j])<=70:
                           data7+=1
                       elif int(kuk_list[j])<=80:
                           data8+=1
                       elif int(kuk_list[j])<=90:
                           data9+=1
                       elif int(kuk_list[j])<=100:
                           data10+=1
                    data_x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                    data_y = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10]
                    values = [' 0~10 ', ' 10~20 ', ' 20~30 ', ' 30~40 ', ' 40~50 ', ' 50~60 ', ' 60~70 ', ' 70~80 ',
                              ' 80~90 ', ' 90~100 ']
                    plt.title("A national language score")
                    plt.ylabel('Number of students', labelpad=15)
                    plt.xticks(data_x, values)
                    plt.xticks(rotation=45)
                    plt.show()
                    print('분석을 이어서 하시겠습니까?')
                    answerxs = input()
                    if answerxs == 'yes':
                        continue
                elif answerlast == '수학':
                    for j in range(39):
                       if int(sulist[j])<=10:
                           data1+=1
                       elif int(sulist[j])<=20:
                           data2+=1
                       elif int(sulist[j])<=30:
                           data3+=1
                       elif int(sulist[j])<=40:
                           data4+=1
                       elif int(sulist[j])<=50:
                           data5+=1
                       elif int(sulist[j])<=60:
                           data6+=1
                       elif int(sulist[j])<=70:
                           data7+=1
                       elif int(sulist[j])<=100:
                           data8+=1
                       elif int(sulist[j])<=90:
                           data9+=1
                       elif int(sulist[j])<=100:
                           data10+=1
                    data_x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                    data_y = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10]
                    values = [' 0~10 ', ' 10~20 ', ' 20~30 ', ' 30~40 ', ' 40~50 ', ' 50~60 ', ' 60~70 ', ' 70~80 ',' 80~90 ', ' 90~100 ']
                    plt.title("Math score")
                    plt.ylabel('Number of students', labelpad=15)
                    plt.xticks(data_x, values)
                    plt.xticks(rotation=45)
                    plt.show()
                    print('분석을 이어서 하시겠습니까?')
                    answerxs = input()
                    if answerxs == 'yes':
                        continue
                elif answerlast == '사회':
                    for j in range(39):
                       if int(salist[j])<=10:
                           data1+=1
                       elif int(salist[j])<=20:
                           data2+=1
                       elif int(salist[j])<=30:
                           data3+=1
                       elif int(salist[j])<=40:
                           data4+=1
                       elif int(salist[j])<=50:
                           data5+=1
                       elif int(salist[j])<=60:
                           data6+=1
                       elif int(salist[j])<=70:
                           data7+=1
                       elif int(salist[j])<=80:
                           data8+=1
                       elif int(salist[j])<=90:
                           data9+=1
                       elif int(salist[j])<=100:
                           data10+=1
                    data_x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                    data_y = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10]
                    values = [' 0~10 ', ' 10~20 ', ' 20~30 ', ' 30~40 ', ' 40~50 ', ' 50~60 ', ' 60~70 ', ' 70~80 ',' 80~90 ', ' 90~100 ']
                    plt.title("Society score")
                    plt.ylabel('Number of students', labelpad=15)
                    plt.xticks(data_x, values)
                    plt.xticks(rotation=45)
                    plt.show()
                    print('분석을 이어서 하시겠습니까?')
                    answerxs = input()
                    if answerxs == 'yes':
                        continue
                elif answerlast == '과학':
                    for j in range(39):
                        if int(sclist[j]) <= 10:
                            data1 += 1
                        elif int(sclist[j]) <= 20:
                            data2 += 1
                        elif int(sclist[j]) <= 30:
                            data3 += 1
                        elif int(sclist[j]) <= 40:
                            data4 += 1
                        elif int(sclist[j]) <= 50:
                            data5 += 1
                        elif int(sclist[j]) <= 60:
                            data6 += 1
                        elif int(sclist[j]) <= 70:
                            data7 += 1
                        elif int(sclist[j]) <= 80:
                            data8 += 1
                        elif int(sclist[j]) <= 90:
                            data9 += 1
                        elif int(sclist[j]) <= 100:
                            data10 += 1
                    data_x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                    data_y = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10]
                    values = [' 0~10 ', ' 10~20 ', ' 20~30 ', ' 30~40 ', ' 40~50 ', ' 50~60 ', ' 60~70 ', ' 70~80 ',' 80~90 ',' 90~100 ']
                    plt.title("Science score")
                    plt.ylabel('Number of students', labelpad=15)
                    plt.xticks(rotation=45)
                    plt.xticks(data_x, values)
                    plt.show()
                    print('분석을 이어서 하시겠습니까?')
                    answerxs = input()
                    if answerxs == 'yes':
                        continue
                elif answerlast == '영어':
                    for j in range(39):
                        if int(enlist[j]) <= 10:
                            data1 += 1
                        elif int(enlist[j]) <= 20:
                            data2 += 1
                        elif int(enlist[j]) <= 30:
                            data3 += 1
                        elif int(enlist[j]) <= 40:
                            data4 += 1
                        elif int(enlist[j]) <= 50:
                            data5 += 1
                        elif int(enlist[j]) <= 60:
                            data6 += 1
                        elif int(enlist[j]) <= 70:
                            data7 += 1
                        elif int(enlist[j]) <= 80:
                            data8 += 1
                        elif int(enlist[j]) <= 90:
                            data9 += 1
                        elif int(enlist[j]) <= 100:
                            data10 += 1
                    data_x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                    data_y = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10]
                    values = [' 0~10 ', ' 10~20 ', ' 20~30 ', ' 30~40 ', ' 40~50 ', ' 50~60 ', ' 60~70 ', ' 70~80 ', ' 80~90 ', '  90~100  ']
                    plt.plot(data_x, data_y, marker="o")
                    plt.title("English score")
                    plt.ylabel('Number of students', labelpad=15)
                    plt.xticks(data_x, values)
                    plt.xticks(rotation=45)
                    plt.show()
                    print('분석을 이어서 하시겠습니까?')
                    answerxs = input()
                    if answerxs == 'yes':
                        continue
    if operation ==4:
        break
