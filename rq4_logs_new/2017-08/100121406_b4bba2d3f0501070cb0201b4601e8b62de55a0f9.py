#!/usr/bin/python
# -* coding:utf-8 -*-
# Python:3.2
# Author:chenminghuaht@foxmail.com
# Version:1.0

import os
import os.path
import urllib
import urllib2
import json

fileName='artifact/MST_838_android/device/changhong/apkinfos/currapkinfos.xml'

def registerUrl():
    try:
        url = 'http://10.3.15.83/jenkins/view/MST938/job/MST938_MP/api/python?pretty=true'
        data = urllib2.urlopen(url).read()
        return data
    except Exception,e:
        print e

def writeJsonFile(fileData):
    file = open("json.txt","w")
    file.write(fileData)
    file.close()

def parserJsonFile(jsonData):
    value = json.loads(jsonData)
    for build in value["builds"]:
        buildUrl=build["url"]
        buildNumber=str(build["number"])
        fileUrl=buildUrl+fileName
        print "file download url is: " + fileUrl
        saveXMLFile(fileUrl,buildNumber)

def genBuildVersion(buildNumber):
    n = buildNumber.zfill(6)
    m = n[0:1] + "." + n[2:]
    print "The build version is: " + m
    return m
	
def saveXMLFile(fileUrl,buildNumber):
    status = urllib.urlopen(fileUrl).code
	
    if(status != 404):
        urllib.urlretrieve(fileUrl,"currapkinfos.xml."+ genBuildVersion(buildNumber))
    else:
        print "the build has no currentapkinfos.xml"


if __name__ == '__main__':
    data = registerUrl()
    handledData = data.replace('None','0').replace('True','1').replace('False','0')
    parserJsonFile(handledData)