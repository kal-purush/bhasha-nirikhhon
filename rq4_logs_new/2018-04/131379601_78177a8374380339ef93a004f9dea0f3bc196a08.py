import sys
import urllib.request
import urllib.parse
import time
import re

#response = urllib.request.urlopen("https://www.imdb.com/title/tt0096895/?ref_=fn_al_tt_1")
nameMovieAll = input("Введите название фильма:");
response = urllib.request.urlopen("https://www.imdb.com/find?ref_nv_sr_fn&q="+nameMovieAll+"&s=all");#Batman&s=all")
html = response.read().decode("utf-8") # utf-8 чтобы принять русские буквы
st0 = [];
st1 = [];
ps1 = html.find("findResult odd",1,len(html))+17;
ps2 = html.find("findMoreMatches",1,len(html))-26;#-27;
data = html[ps1:ps2];

st11 = "https://www.imdb.com";
while len(data) >0:
    ps1 = data.find("<td",0,len(data));
    ps2 = data.find("td>")+3;
    st0.append(data[ps1:ps2]);
    data = data[ps2:len(data)];

for i in range(1,len(st0),2):
    data = st0[i];
    ps1 = data.find("href",0,len(data))+6;
    ps2 = data.find(" ",ps1,len(data))-1;
    st1.append(st11 + data[ps1:ps2]);


print (ps1);
print (ps2);
print (len(st0));
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

ps1 = html.find('<div class="title_wrapper">',1,len(html));#originalTitle",1,len(html))-250;
ps2 = html.find(">See full cast & crew</a>",1,len(html))-26;#-27;
data = html[ps1:ps2];

print(ps1);
print(ps2);

print (data);

#open("hakaton.txt","w").write(html);
f=open("hakaton.txt","w", encoding="utf-8");
f.write(html);
f.close();