# -*- conding:utf-8 -*-

PATH = 'D:\\spider data\\'

def put(content, ending='\n', file='log.txt'):
    with open(PATH + file, 'a') as file:
        file.write(str(content) + '\n')
