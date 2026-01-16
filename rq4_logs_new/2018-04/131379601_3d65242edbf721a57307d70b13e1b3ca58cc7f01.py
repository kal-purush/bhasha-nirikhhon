#!/usr/bin/env python
# coding=utf-8
# -*- coding: utf-8 -*-
# Устанавливаем стандартную внешнюю кодировку = utf8

import sys
import urllib.request
import requests
#import urllib.parse
import time
import re
from urllib.parse   import quote

def is_rus(text):
    match = re.match('[а-яА-ЯёЁ]',text);#"""^[а-яА-ЯёЁ][а-яё0-9 !?:;"'.,]+$""", text)
    return bool(match)

nameMovieAll = input("Введите название фильма:");
s_url_en = "https://www.imdb.com/find?ref_nv_sr_fn&q="+nameMovieAll+"&s=all";
s_url_rus = "https://www.imdb.com/find?ref_=nv_sr_fn&q="+nameMovieAll+"&s=all";

# Возможность использования русских названий фильмов
if is_rus(nameMovieAll):
    s_url_rus = "https://www.imdb.com/find?ref_=nv_sr_fn&q="+quote(nameMovieAll)+"&s=all";
    s = s_url_rus;
else:
    s = s_url_en;
    
#s = s_url_en;
response = urllib.request.urlopen(s);

html = response.read().decode("utf-8") # utf-8 чтобы принять русские буквы
st0 = [];
st1 = [];
ps1 = html.find('<tr class="findResult odd">',1,len(html))+17;
ps2 = html.find("findMoreMatches",1,len(html))-26;#-27;
data = html[ps1:ps2];

st11 = "https://www.imdb.com";
while len(data) >0:
    ps1 = data.find('<td class="result_text">',0,len(data));
    ps2 = data.find("td>")+3;
    st0.append(data[ps1:ps2]);
    data = data[ps2:len(data)];

for i in range(1,len(st0),2):
    data = st0[i];
    ps1 = data.find("href",0,len(data))+6;
    ps2 = data.find(" ",ps1,len(data))-1;
    st1.append(st11 + data[ps1:ps2]);
    


l = int(len(st0)/2);
print ("Количество фильмов: "+str(l));


for i in range(1,len(st0)-1,2):
    print (st0[i]);
for i in range(l):
    print (st1[i]);

nMovie = input("Введите номер фильма:");
urlMovie = st1[int(nMovie)-1];
response = urllib.request.urlopen(urlMovie);
html = response.read().decode("utf-8") # utf-8 чтобы принять русские буквы

ps1 = html.find('<div class="title_wrapper">',1,len(html));
ps2 = html.find(">See full cast & crew</a>",1,len(html))-26;
data = html[ps1:ps2];

stMovieResult=[];
stMovieResultDesc=['Название: ','Оригинальное название: ','Возрастные ограничения: ','Продолжительность: ','Жанр: ','Дата выхода на экран: ','Ссылка на постер: ','Краткое описание: ','Режиссер: ','Сценарий: ','Актерский состав: '];
# Название
s1 = '<h1 itemprop="name" class="">';
s2 = '&nbsp;';
ps1 = data.find(s1,1,len(data))+len(s1);
ps2 = data.find(s2,ps1,len(data));
stMovieResult.append(data[ps1:ps2]);
#print(ps1);
#print(ps2);
#print (data);
data = data[ps2:len(data)];
# Год выхода
#s1 = '\n>';
#s2 = '</a>';
#ps1 = data.find(s1,1,len(data))+len(s1);
#ps2 = data.find(s2,ps1,len(data));
#stMovieResult.append(data[ps1:ps2]);
#data = data[ps2:len(data)];
# Название на английском
s1 = 'originalTitle">';
s2 = '<span';
ps1 = data.find(s1,1,len(data))+len(s1);
ps2 = data.find(s2,ps1,len(data));
if ps1 >= len(s1):
    stMovieResult.append(data[ps1:ps2]);
    data = data[ps2:len(data)];
else:
    stMovieResult.append('');
    
# Rating
s1 = 'itemprop="contentRating" content="';
s2 = '"';
ps1 = data.find(s1,1,len(data))+len(s1);
ps2 = data.find(s2,ps1,len(data));
if ps1 >= len(s1):
    stMovieResult.append(data[ps1:ps2]);
    data = data[ps2:len(data)];
else:
    stMovieResult.append('');

# Продолжительность
s1 = 'itemprop="duration" datetime="PT';
s2 = 'M">';
ps1 = data.find(s1,1,len(data))+len(s1);
ps2 = data.find(s2,ps1,len(data));
stMovieResult.append(data[ps1:ps2]+' мин');
data = data[ps2:len(data)];

# Считываем жанры
stGenre = [];
s1 = 'genre">';
s2 = '</span>';
ps1 = data.find(s1,1,len(data))+len(s1);
while ps1 >= len(s1):
    ps2 = data.find(s2,1,len(data));
    stGenre.append(data[ps1:ps2]);
    data = data[ps2:len(data)];
    ps1 = data.find(s1,1,len(data))+len(s1);

s = ', '.join(stGenre);
stMovieResult.append(s[2:len(s)]);

# Дата выхода фильма на экраны
s1 = 'release dates" >';
s2 = '\n';
ps1 = data.find(s1,1,len(data))+len(s1);
ps2 = data.find(s2,ps1,len(data));
stMovieResult.append(data[ps1:ps2]);
data = data[ps2:len(data)];

# Ссылка на постер
s0 = '<div class="poster">';
ps1 = data.find(s1,1,len(data))+len(s1);
s1 = 'src="';
s2 = '"';
ps1 = data.find(s1,ps1,len(data))+len(s1);
ps2 = data.find(s2,ps1,len(data));
stMovieResult.append(data[ps1:ps2]);
data = data[ps2:len(data)];
# Описание фильма
s1 = 'itemprop="description">\n                    ';
s2 = '\n';#\n\n            </div>';
ps1 = data.find(s1,1,len(data))+len(s1);
ps2 = data.find(s2,ps1,len(data));
stMovieResult.append(data[ps1:ps2]);
data = data[ps2:len(data)];
# Режиссер
s1 = '<span itemprop="creator"';
ps1 = data.find(s1,1,len(data))+len(s1);
if ps1 >= len(s1):
    s1 = 'itemprop="name">';
    s2 = '</span>';
    ps1 = data.find(s1,ps1,len(data))+len(s1);
    ps2 = data.find(s2,ps1,len(data));
    stMovieResult.append(data[ps1:ps2]);
    data = data[ps2:len(data)];
else:
    s1 = '<span itemprop="director"';
    ps1 = data.find(s1,1,len(data))+len(s1);
    if ps1 >= len(s1):
        s1 = 'itemprop="name">';
        s2 = '</span>';
        ps1 = data.find(s1,ps1,len(data))+len(s1);
        ps2 = data.find(s2,ps1,len(data));
        stMovieResult.append(data[ps1:ps2]);
        data = data[ps2:len(data)];
    else:
        stMovieResult.append(data[ps1:ps2]);

# Сценаристы
s1 = '<span itemprop="writers"';
s2 = 'class="ghost">';
ps1 = data.find(s1,1,len(data))-10;
ps2 = data.find(s2,1,len(data));
k = ps2;
if ps1 >= len(s1):
    data1 = data[ps1:ps2];
    stWriters = [];
    s1 = 'itemprop="name">';
    s2 = '</span>';
    ps1 = data1.find(s1,1,len(data))+len(s1);
    while ps1 >= len(s1):
        ps2 = data1.find(s2,ps1,len(data1));
        stWriters.append(data1[ps1:ps2]);
        data1 = data1[ps2:len(data1)];
        ps1 = data1.find(s1,1,len(data1))+len(s1);
    s = ', '.join(stWriters);
    data = data[k:len(data)];
else:
    s = '';

stMovieResult.append(s[0:len(s)]);

#print (data);
# Актеры

s1 = '<span itemprop="actors"';
s2 = 'class="ghost">';
ps1 = data.find(s1,1,len(data));
ps2 = data.find(s2,ps1,len(data));
k = ps2;
if ps1 >= len(s1):
    data1 = data[ps1:ps2];
    stActors = [];
    s1 = 'itemprop="name">';
    s2 = '</span>';
    ps1 = data1.find(s1,1,len(data))+len(s1);
    while ps1 >= len(s1):
        ps2 = data1.find(s2,ps1,len(data1));
        stActors.append(data1[ps1:ps2]);
        data1 = data1[ps2:len(data1)];
        ps1 = data1.find(s1,1,len(data1))+len(s1);
    s = ', '.join(stActors);
    data = data[k:len(data)];
else:
    s = '';

stMovieResult.append(s[0:len(s)]);

# Вывод на экран
print('\n');
for i in range(len(stMovieResult)):
    if stMovieResult[i] != '':
        s1 = stMovieResultDesc[i];
        s2 = stMovieResult[i];
        s1 = s1.encode('utf8');
        s2 = s2.encode('utf8');
        #s1 = u'%s' % u''.join(s1);
        #s2 = u'%s' % u''.join(s2);
        #s3 = s1+s2;
        print (s1+s2);

# Запись описание фильма в текстовой файл
f=open("hakaton.txt","w", encoding="utf-8");
for i in range(len(stMovieResult)):
    f.write(stMovieResultDesc[i]+stMovieResult[i]+'\n');
f.close();