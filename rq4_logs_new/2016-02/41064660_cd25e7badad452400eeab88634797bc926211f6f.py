## Elias Haddad ehaddad@splunk.com
import os
import sys
import urllib2
import re




def getLatestSPL(TA_HOME, ta):
	endpoint=TA_HOME + str(ta) + '/builds/develop/latest'
	try:
		response = urllib2.urlopen(endpoint)
		html = response.read()
		RList= re.findall(r">([0-9a-zA-Z\-_\.]+\.[a-zA-Z]{3})</", html)
		#get latest 
		for s in RList:
			package_name=s

		return package_name

	except Exception, e:
		print "Error - Coud not get SPL. Error=" + str(e) + " TA=" + str(ta) + " endpoint=" + endpoint
		return -1

def downloadLatestSPL(SPLUNK_HOME, TA_HOME, ta, spl):
	endpoint= TA_HOME + str(ta) + '/builds/develop/latest/'  + str(spl)
	try:
		response = urllib2.urlopen(endpoint)
		spl_file = response.read()
		file_name= re.sub(r"\.spl|\.tgz", "", str(spl))
		path_file=SPLUNK_HOME + "/etc/apps/" + file_name
		file = open(path_file, "w")
		file.write(spl_file)
		file.close()
		return endpoint

	except Exception, e:
		print "Error - Coud not save and download spl file. Error=" + str(e) + " TA=" + str(ta) + " endpoint=" + endpoint
		return -1

def extractTA(SPLUNK_HOME, spl):
	try:
		file_name= re.sub(r"\.spl|\.tgz", "", str(spl))
		folder= re.sub(r"\-[0-9]+\.[0-9]+[^a-zA-Z]+", "", file_name)
		str1="rm -Rf " + SPLUNK_HOME + "/etc/apps/" + folder
		str2="rm -f " + SPLUNK_HOME + "/etc/apps/" + folder + "*.tar.gz"
		str3="mv " + SPLUNK_HOME + "/etc/apps/" +  file_name + " " + SPLUNK_HOME + "/etc/apps/" +  file_name + ".tar.gz"
		str4= "tar zxvf " + SPLUNK_HOME + "/etc/apps/" + file_name + ".tar.gz -C " + SPLUNK_HOME + "/etc/apps/"

		os.system(str1)
		os.system(str2)
		os.system(str3)
		os.system(str4)
		return folder

	except Exception, e:
		print "Error - Coud not extract tar file. Error=" + str(e)

def restartSplunk():
	str= "/Applications/Splunk/bin/splunk restart"
	os.system(str)

	
SPLUNK_HOME = os.environ['SPLUNK_HOME']
#TA_HOME='https://artifactory01.sv.splunk.com/artifactory/simple/Solutions/TA/'
TA_HOME="http://repo.splunk.com/artifactory/Solutions/TA/"


try:
	response = urllib2.urlopen(TA_HOME)
	html = response.read()
	TList= re.findall(r"(TA\-[0-9a-zA-Z\-_]+)/<", html)
	for s in TList:
		print "Starting TA=" + s
		latestPackage= getLatestSPL(TA_HOME, s)
		if (latestPackage!=-1):
			#if file does not exist - meaning there is a new release
			fname=SPLUNK_HOME+ '/etc/apps/' + re.sub(r"\.spl|\.tgz", "", latestPackage) + ".tar.gz"
			if (os.path.isfile(fname)):
				print "Latest build=" + re.sub(r"\.spl|\.tgz", "", latestPackage) + ".tar.gz already exists. Skipping update"
			else:
				url=downloadLatestSPL(SPLUNK_HOME, TA_HOME, s, latestPackage)
				folder=extractTA(SPLUNK_HOME, latestPackage)
				print "Sucessfully Updated TA. TA=" + s + " from URL=" + url + " folder=" + folder
	#restartSplunk()


except Exception, e:
	print "Error - Unable to run program. Error=" + str(e)









