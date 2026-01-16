#!/usr/bin/env python
# encoding: utf-8
# ===============================================================================
#
#         FILE:
#
#        USAGE:
#
#  DESCRIPTION:
#
#      OPTIONS:  ---
# REQUIREMENTS:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:  YOUR NAME (),
#      COMPANY:
#      VERSION:  1.0
#      CREATED:
#     REVISION:  ---
# ===============================================================================

import time
from functools import wraps

def count_run_time(func):
    """
    计时修饰器

    :param func:
    :return:
    """
    @wraps(func)
    def _func(*args, **kwargs):
        _start = time.time()
        rt = func(*args, **kwargs)
        print("[func: %s] cost @%.6fs" % (func.__name__, time.time() - _start))
        return rt
    return _func

def example_1(ss: int, lyst: list):
    print("working")

def example_2(ss: int, lyst: list) -> int:
    print("working")

def example_3(ss: int, lyst: list):
    """
    33. some doc

    :param s1:
    :return:
    """
    print("working")

def example_4(ss: int, lyst: list) -> int:
    """
    3.3.1 搜索最小值

    :param lyst:
    :return:
    """
    print("working")

@count_run_time
def example_5(ss: int, lyst: list):
    print("ss ok, lyst not, abc lyst??")

@count_run_time
def example_6(ss: int, lyst: list) -> int:
    print("ss ok, lyst not, abc lyst??")

@count_run_time
def example_7(ss: int, lyst: list):
    """
    33. some doc

    :param s1:
    :return:
    """
    print("ss not, lyst not, abc lyst??")

@count_run_time
def example_8(ss: int, lyst: list) -> int:
    """
    3.3.1 搜索最小值

    :param lyst:
    :return:
    """
    print("ss not, lyst not, abc lyst??")