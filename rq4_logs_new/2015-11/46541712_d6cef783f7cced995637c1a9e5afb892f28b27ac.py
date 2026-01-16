#coding=utf-8
import threading
import urllib2
import re
import Queue
def getpapaer(strs):
	wybug_title="<h3 class='wybug_title'>(.*)</h3>"
	wybug_corp='<a href="http://www.wooyun.org/corps/(.*)">'
	wybug_author='<a href="http://www.wooyun.org/whitehats/(.*)">'
	wybug_date="<h3 class='wybug_date'>(.*)</h3>"
	wybug_open_date="<h3 class='wybug_open_date'>(.*)</h3>"
	wybug_type="<h3 class='wybug_type'>(.*)</h3>"
	wybug_level="<h3 class='wybug_level'>(.*)</h3>"
	wybug_cotents="<div class='wybug_detail'>([\s\S]*?)<h3 class=\"detailTitle\">修复方案：</h3>"
	#print "http://wooyun.org%s"%strs
	data=urllib2.urlopen("http://wooyun.org%s"%strs).read()
	wybug_title= re.findall(wybug_title,data)[0].split("：		")[1].replace('	','')
	wybug_corp= re.findall(wybug_corp,data)[0]
	wybug_author= re.findall(wybug_author,data)[0]
	wybug_date= re.findall(wybug_date,data)[0].split("：		")[1]
	wybug_open_date= re.findall(wybug_open_date,data)[0].split("：		")[1]
	wybug_type= re.findall(wybug_type,data)[0].split("：		")[1]
	wybug_level= re.findall(wybug_level,data)[0].split("：		")[1]
	wybug_cotents=re.findall(wybug_cotents,data)
	if len(wybug_cotents)!=0:
		wybug_cotents=wybug_cotents[0]
	else:
		wybug_cotents="null"
	data=open('temp.html').read()
	data=data.replace("{wybug_title}",wybug_title)
	data=data.replace("{wybug_corp}",wybug_corp)
	data=data.replace("{wybug_author}",wybug_author)
	data=data.replace("{wybug_date}",wybug_date)
	data=data.replace("{wybug_open_date}",wybug_open_date)
	data=data.replace("{wybug_type}",wybug_type)
	data=data.replace("{wybug_level}",wybug_level)
	data=data.replace("{wybug_cotents}",wybug_cotents)
	name=strs.split("/")[2]
	
	open('bugs/'+name+".txt",'w').write(data)
	#print data