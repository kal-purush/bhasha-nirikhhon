import requests as req
import sys
import re
from time import sleep
# import random
# from bs4 import BeautifulSoup
import os 

WCD = os.getcwd()

def makeNewDirIfNecessary(major):
	path = WCD + '/pdf/' + major + '/'
	try: 
	    os.makedirs(path)
	except OSError:
	    if not os.path.isdir(path):
	        raise
	# print path
	return

def crawler(coe,las,Wait):
	with open(coe) as f:
		COEurlList = f.read().split()
	with open(las) as f:
		LASurlList = f.read().split()

	maxWait = int(Wait)

	# pdf link can be found at 'https://my.sa.ucsb.edu/catalog/Current/CollegesDepartments/ls-intro/stats.aspx?DeptTab=Courses'
	LASPattern = re.compile(r'/ls-intro/(.*)\.aspx')
	COEPattern = re.compile(r'/coe/(.*)\.aspx')
	HOST = 'https://my.sa.ucsb.edu/'
 
	linkPattern = re.compile(r'\"\/(.*?)\.pdf')
	pdfNamePattern = re.compile(r'.*\w+\/\w+\/\w+\/\w+\/\w+\/\w+\/(.*\.pdf)')
	majorPattern = re.compile(r'.*\w+\/\w+\/\w+\/\w+\/\w+\/(.*)\/.*')

	# check LAS loop comments, COE is relatively simple
	for url in COEurlList:
		href = url[:-7] + "Undergraduate"
		res = req.get(href).text
		major = re.findall(COEPattern,href)[0]
		link = HOST[:-1] + re.search(linkPattern,res).group(0)[1:]
		makeNewDirIfNecessary(major)
		path = WCD + '/pdf/' + major + '/' + re.split(COEPattern,url)[1] + '.pdf'

		with open (path, 'wb') as pdf:
			pdf.write(req.get(link).content)
		sleep(maxWait)

	for url in LASurlList:
		res = req.get((url[:-7] + "Undergraduate")).text

		# one department might have multiple major and minor, thus use re.findall
		hrefList = re.findall(linkPattern,res)

		# get major from url e.g, 'Math'
		major = re.findall(majorPattern,hrefList[0])[0]
		# /Users/Shawn/Desktop/SaberCourse/pdf/Math/
		makeNewDirIfNecessary(major)

		hrefList = [HOST + href + '.pdf' for href in hrefList]

		# create new directories and prepare filenames
		pdfNameList = [ WCD + '/pdf/' + major + '/' + re.findall(pdfNamePattern,href)[0] for href in hrefList]

		for i in range(len(hrefList)):
		  	with open (pdfNameList[i], 'wb') as pdf:
				pdf.write(req.get(hrefList[i]).content)
		      	#print re.split(LASPattern,hrefList[i])
		sleep(maxWait)


if (len(sys.argv) != 4):
	print("Error: Invalid Filename, Expecting 2.txt file and maximum wait time ")
else:
	crawler(sys.argv[1],sys.argv[2],sys.argv[3])

# python2 getMajorPDF.py COE_URL.txt LAS_URL.txt 2