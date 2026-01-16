#coding:utf-8

"""
����ṹʵ��
"""

import sys #����һ��'ϵͳ'ģ��,��ʹ��'ϵͳ'�ṩ�Ĺ���

hi_words = 'Hi!'  #�����������õ�һ�����ֶ������

class CE(object):
    def __init__(self, name):
        self.name = name

    def say_hello(self):
        words = '%s ! My name is %s .' % (hi_words, self.name)
        print(words)

def hi_ce():    #������һ������(����) , �����������һ������
    peter = CE('peter')
    peter.say_hello()

if __name__ == '__main__':
    hi_ce()