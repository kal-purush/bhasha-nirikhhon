# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 19:05:05 2016

@author: Robert
"""

#from pymystem3 import Mystem
import bs4
import urllib3
import json
import re
import time
import os


# Программа запускается одним большим куском, сначала обкачивает сайт газеты в память (в виде словаря),
# затем создает соответствующие директории вида (каталог, где находится скрипт)/год/месяц/... ,
# сохраняет в них файлы в неразмеченном виде,
# для каждого из них вызывает майстем (нужно указать путь к исполняемому файлу) и прогоняет два раза - с выдачей в xml и txt.
# Создать csv можно, вызвав функцию makecsv() - после того, как весь файл хотя бы один раз запустится и в памяти останется словарь.




http = urllib3.PoolManager()

gazeta = http.request('GET', 'http://www.zeml-trub.ru/index.php')
pagedata = bs4.BeautifulSoup(bs4.UnicodeDammit(gazeta.data, is_html = True).unicode_markup, 'lxml')

timedict = {}
datelist = []
#rgxp = "[0-9]+\-[0-9]+\-[0-9]+"
#rgxp = '2'
counter = 0

# Определяем количество страниц со статьями для обхода

Nstranic = re.findall('[0-9]+ ', pagedata.find(string = 'дальше').parent.parent.get_text())[-1].strip()

for stranica in range(1, int(Nstranic)):
    for i in pagedata.find_all('div'):
        
        try:
            if i.get('class')[0] == 'readmore':
                counter = counter + 1
                stranica = http.request('GET',i.a['href'])
                stranica = bs4.BeautifulSoup(bs4.UnicodeDammit(stranica.data, is_html = True).unicode_markup, 'lxml')
                a = stranica.find_all('a')
                tempdict = {}
                try:
                    for q in a:
                        if q.get('onclick') != None:
                            if q['onclick'][0:5] == 'ShowP':
                                author = q.get_text()
 #                               print(author)
                                date = re.findall("[0-9]+\-[0-9]+\-[0-9]+", q.parent.get_text())
                                
                                if len(re.findall("[0-9]+\-[0-9]+\-[0-9]+", q.parent.get_text())) == 0:
                                    subdate = re.findall('Сегодня', q.parent.get_text())
                                    vremya = time.gmtime()
                                else:
                                    vremya = time.strptime(date[0], '%d-%m-%Y')
                                tempdict.update({'author' : author, 'date' : time.strftime('%d.%m.%Y', vremya), 
                                                 'header' : re.findall('.*?\»', stranica.title.string)[0],
                                                 'day' : time.strftime('%d', vremya),
                                                 'weekday' : time.strftime('%A', vremya),
                                                 'month' : time.strftime('%B', vremya),
                                                 'year' : time.strftime('%Y', vremya),
                                                 'link' : i.a['href']})
                                
                except:
                    pass            
                for q in stranica.find_all('div'):
                    try:
                        if q.get('class')[0] == 'full_story':
                            tempdict.update({'text' : q.get_text()})
                            timedict.update({'post' + str(counter) : tempdict})
                            print('post' + str(counter) + ' ' + timedict['post'+ str(counter)]['year'] + ' ' + timedict['post'+ str(counter)]['month'])
                    except TypeError:
                        pass
        except:
            pass
    gazeta = http.request('GET', pagedata.find(string = 'дальше').parent['href'])
    pagedata = bs4.BeautifulSoup(bs4.UnicodeDammit(gazeta.data, is_html = True).unicode_markup, 'lxml')
    

# Опционально - сохранить словарь в jsone
#y = open('F:\\dumpling.txt', 'w', encoding = 'utf-8')
#json.dump(timedict,y,ensure_ascii = False, indent = 2)
#y.close()
            
for item in timedict:
    print(item)
    filepath = timedict[item]['year'] + '\\' + timedict[item]['month'] + '\\'
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    gg = open(filepath + item + '.txt', 'w+', encoding = 'utf-8')
    cash = '@' + timedict[item]['author'] + '\n' + '@' + timedict[item]['header'] +'\n' + '@' + timedict[item]['date'] + '\n' +'@' + timedict[item]['link'] + '\n' + timedict[item]['text']
    gg.write(cash)
    gg.close()
    cwd = os.getcwd()
    os.system(r'C:\Users\Robert\Desktop\specialforpath\mystem.exe -dige utf-8 --format xml '+ cwd + '\\' + filepath + item + '.txt' + ' ' + cwd + '\\' + filepath + item + '.xml')
    os.system(r'C:\Users\Robert\Desktop\specialforpath\mystem.exe -dige utf-8 --format text '+ cwd + '\\' + filepath + item + '.txt' + ' ' + cwd + '\\' + filepath + item + '_plaintext.txt')
        
def makecsv():
      
    csvfile = open('metatable.csv', 'w+', encoding = 'utf-8')
    csvhead = 'path;author;sex;birthday;header;created;sphere;genre_fi;'
    csvhead = csvhead + 'type;topic;chronotop;style;audience_age;audience_level;audience_size;source;'
    csvhead = csvhead + 'publication;publisher;publ_year;medium;country;region;language;'
    formcsv = csvhead + '\n' 
    for item in timedict:
        filepath = timedict[item]['year'] + '\\' + timedict[item]['month'] + '\\' + item + '.txt'  
        csvrow = filepath + ';' + timedict[item]['author'] + ';;;' + timedict[item]['header'] + ';' 
        csvrow = csvrow + timedict[item]['date'] + ';' + 'Публицистика;;;;;' + 'нейтральный;'+ 'н-возраст;н-уровень;районная;'
        csvrow = csvrow + timedict[item]['link'] + ';' + 'Земля Трубчевская;;' + timedict[item]['year'] + ';'  
        csvrow = csvrow + 'газета;Россия;Трубчевский район Брянской области;ru;' + '\n'
        print(csvrow)    
        formcsv = formcsv + csvrow
    csvfile.write(formcsv)
    csvfile.close()

    