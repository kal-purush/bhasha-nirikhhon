# scriptsimport urllib2
import praw
import urllib
from urlparse import urljoin
from os.path import expanduser
from bs4 import BeautifulSoup as bs
import os
import requests
home = expanduser("~")+"/Desktop/"
if not os.path.exists(home+"downloads"):
    os.makedirs(home+"downloads")
def dl(each):
	filename=each.split('/')[-1]
	urllib.urlretrieve(each, filename)

def dligifv(each):
	filename=each.split('/')[-1]
	each1 = each.split(".")
	each1[-1] = "webm"
	urllib.urlretrieve(".".join(each1), home+"/downloads/"+filename)

def dlalbum(x):
	response = requests.get(x+"/all")
	soup =  bs(response.text,"lxml")
	listimg = soup.find(id="imagelist")
	directory = home+"downloads/"+x.split("/")[-1]
	if not os.path.exists(directory):
	    os.makedirs(directory)
	images = listimg.find_all("a",href=True)
	for i in range(len(images)):
		try:
			# print images[i]['href']
			iimgurdl(urljoin(x,images[i]['href']),directory,i)	
		except:
			pass

def gfydl(link):
	response = requests.get(link)
	soup = bs(response.text,"lxml")
	each = soup.find(id="mp4Source")['src']
	filename=each.split('/')[-1]
	mp3file = urllib2.urlopen(each)
	with open(home+"/downloads/"+filename,'wb') as f:
		f.write(mp3file.read())


def iimgurdl(link,directory=None,i=None):
	# print link,directory,i
	if "gifv" in link:
		dligifv(link)

	else:
		if directory and i:
			filename=link.split('/')[-1]
			if len(filename.split(".")[0])>8 and filename.split(".")[0][-1]=="b":
				pass
			else:	
				fpath =  directory+"/"+str(i)+"_"+filename
				urllib.urlretrieve(link,fpath)
		else:
			filename=link.split('/')[-1]
			fpath =  home+"/downloads/"+filename
			urllib.urlretrieve(link,fpath)
					
def imgurDL(link):
	if "i.imgur" in link:
		iimgurdl(link)
	elif "imgur.com/a/" in link:
		dlalbum(link)
	elif "imgur" in link:
		pass	 	
def typeget(link):
	if "imgur" in link:
		imgurDL(link)
	elif "gfycat" in link:
		gfydl(link)	

r = praw.Reddit(user_agent='my_cool_application')
submissions = r.get_subreddit('holdthemoan').get_top(limit=100,params={"t":"year"})
for i in submissions:
	print i.url
	typeget(i.url)