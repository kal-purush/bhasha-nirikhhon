# -*- coding: utf-8 -*-

from random import randint

# 一轮里面需要几次猜对
right_times_in_one_round = []
# 猜对赋1，猜错赋0，添加到result里
result = []

while True:
    times = input('How many times do you want to guess in one round ?\n')
    times_for_statistics = times
    Com = randint(0, 9)

    while times > 0:
        Guess = input('Guess a number from 0 to 9:\n')
        if Guess < Com:
            print "Too Small!\n"
        elif Guess > Com:
            print "Too Big!\n"
        else:
            print "You are right!\n"
            # 算出目前第几轮
            which_times = times_for_statistics + 1 - times
            # 添加到此list中
            right_times_in_one_round.append(which_times)
            # 猜对将结果添加到result中
            result.append(1)
            break
        times = times - 1
    # 机会用完都没有猜对，result添加0，一轮里面1次都没有猜对，添加总次数
    if times == 0:
        right_times_in_one_round.append(times_for_statistics)
        result.append(0)
    # 平均值，猜对所有次的总数除以result的和（猜对了几次），前面和转换到了float类型
    if sum(result) != 0:
        avg = float(sum(right_times_in_one_round)) / sum(result)
        Round = len(result)
        print '已经猜了几轮：%d' % Round
        print '平均几次猜中：%.2f' % avg
    else:
        Round = len(result)
        print '已经猜了几轮：%d' % Round
        print '还没猜中'