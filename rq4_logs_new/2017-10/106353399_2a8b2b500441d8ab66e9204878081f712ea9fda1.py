#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/9/2017 8:35 AM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : AX.py
# @Software: PyCharm Community Edition

from datetime import datetime, date, timedelta
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='./logs/AX.log',
                    filemode='w')


def getWeekDay():
    day = datetime.now().weekday()
    return day


def monthDeadline():
    result = date.isoformat(datetime.today())
    return result


def isFriday():
    day = getWeekDay()
    if day == 4:
        return True
    else:
        return False


def respone_isFriday():
    if isFriday():
        result = u'今天是礼拜五，请及时注 AX !'
        return result
    else:
        day = getWeekDay()
        delta = 4 - day
        deltaDay = timedelta(days=delta)
        forcastDay = datetime.now() + deltaDay
        result = u'这个礼拜请在 {0} 之前注AX !'.format(date.isoformat(forcastDay))

        return result


def isMonthDeadline():
    day = monthDeadline()
    days = ['2017-10-27', '2017-11-28', '2017-12-27']
    if day in days:
        return True
    else:
        return False


def response_isMonthDeadline():
    now = datetime.now()
    month = now.month
    days = ['2017-10-27', '2017-11-28', '2017-12-27']
    if isMonthDeadline():
        result = u'今天是财月的最后一天，请及时注 AX !'
        return result
    else:
        if month == 10:
            delat = 27 - now.day
            if delat < 3:
                result = u'这个礼拜是月结日，请在 {0} 之前注AX !'.format(days[0])
                return result
        elif month == 11:
            delat = 28 - now.day
            if delat < 3:
                result = u'这个礼拜是月结日，请在 {0} 之前注AX !'.format(days[1])
                return result
        elif month == 12:
            delat = 27 - now.day
            if delat < 3:
                result = u'这个礼拜是月结日，请在 {0} 之前注AX !'.format(days[2])
                return result
        else:
            result = u'2017年已过完，请更新2018 AX 日期'
            return result