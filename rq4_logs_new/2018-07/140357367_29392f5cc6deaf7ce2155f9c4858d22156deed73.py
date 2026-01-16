#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  dataDownloader.py
#  
#  Copyright 2018 Philippe Solodov
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  

def main(args):
    return 0
   
class CourseInformation:
    def __init__ (self,name):
        self.name = name
        self.link = ""
        self.title = ""
        self.description = ""
        self.code = ""
        self.credits = 0
        self.prereqs = ""

    def set_link (self, link):
        self.link = link

    def set_title (self, title):
        self.title = title

    def set_description (self, description):
        self.description = description

    def set_code (self, code):
        self.code = code

    def set_credits (self,credits):
        self.credits = credits

    def set_prereqs (self, prereqs):
        self.prereqs = prereqs
        
def getLinks(url,reCompile = "/cs/", mode = 'href'):
	from BeautifulSoup import BeautifulSoup
	import urllib2
	import re

    # Gets all links from page

    html_page = urllib2.urlopen(url)
    soup = BeautifulSoup(html_page)

    links = []
    if mode == 'href':
        for link in soup.findAll('a', attrs={'href': re.compile(reCompile)}):
            links.append(link.get('href'))
 
    elif mode == 'text':
        for link in soup.findAll('a', attrs={'href': re.compile(reCompile)}):
            links.append(link.text)
    return links

def getTitle(url):
	from BeautifulSoup import BeautifulSoup
	import urllib2
	import re
	
    # Fetches course title

    html_page = urllib2.urlopen(url)
    soup = BeautifulSoup(html_page)

    title = soup.find('h4', attrs={})
    title = title.text
    title = title.split(" ")
    title = title[2:]
    title = " ".join(title)

    return title


def getParas(url):
	from BeautifulSoup import BeautifulSoup
	import urllib2
	import re
	
    # Fetches all paragraphs from page
    html_page = urllib2.urlopen(url)
    soup = BeautifulSoup(html_page)
    paras = []

    for para in soup.findAll('p', attrs={}):
        paras.append(para)

    return(paras)

def getCourseCodes(subjectCode):

    # Using UBC's subject link, this will fetch all the course codes from the page
    
    baseLink = "https://courses.students.ubc.ca/cs/main?pname=subjarea&tname=subjareas&req=1&dept="
    subjectLink = baseLink + subjectCode
    print(subjectLink)
    courseCodes = []

    for link in getLinks(subjectLink, mode = 'href'):

        link = link.split("=")

        if link[-1][0:3].isdigit() and len(link[-1]) >= 3:
            
            courseCodes.append(str(link[-1]))

    return courseCodes

def getCourseInformation(department, courseCode, courseInformation = None):

    if not courseInformation: # If no information data is already given, we generate an object to store it
        courseInformation = CourseInformation(str(department) + " " + str(courseCode))

    # Uses the course codes to navigate to course page and fetch course title, course description, course credits, and course pre-reqs
    baseLink = "https://courses.students.ubc.ca/cs/main?pname=subjarea&tname=subjareas&req=3&dept="
    courseLink =  baseLink + department + "&course=" + courseCode
    courseInformation.set_link(courseLink)
    
    courseTitle = getTitle(courseLink)
    courseInformation.set_title(courseTitle)

    paraObjects = getParas(courseLink)
    paras = []

    for para in paraObjects:
        paras.append(para.text)

    if paras[0][0:7] == "Credits":

        # If no course description is provided, first paragraph is credits. Probably.
    
        courseDescription = "None"
        courseCredits = paras[0].split(" ")
        courseCredits = int(courseCredits[1])

    else:
        courseDescription = paras[0]
        courseCredits = paras[1].split(" ")
        courseCredits = courseCredits[1]
    # Sets course pre-reqs

    try:

        # Initializes variables

        n = 0
        coursePreReqs = False

        # Pre-req should always be in 3rd or 2nd paragraphs, unless there are no pre reqs

        for i in range(1,3):
            if "Pre-reqs:" in paras[i]:
                n = i
                coursePreReqs = True
                break


        # This code runs if there are course pre-reqs
            
        if coursePreReqs:
            para = paraObjects[n]
            coursePreReqs = []


            ## This strips the link text from the para objects. This is because pre-reqs are linked to,
            ## and it makes it possible to use BeautifulSoup to find all the course pre-reqs.

            #for link in para.findAll("a", attrs={}):
            #    coursePreReqs.append(link.text)
            
            # ^ Only problem is when courses are optional - does not note this
            
            # Not all pre-requisites come as courses. Some may be faculty approval and such, which
            # requires the content of the pre-req paragraph

            if not coursePreReqs:
                coursePreReqs = paras[n][9:].strip()

    # There may be too few paragraphs because there is both no description or pre-req for the course.
    except IndexError:
        coursePreReqs = False

    courseInformation.set_code(courseCode)
    courseInformation.set_description(courseDescription)
    courseInformation.set_credits(courseCredits)
    courseInformation.set_prereqs(coursePreReqs)
    return courseInformation

def subjectsToCSV():
    dataDump = []
    for link in getLinks("http://www.calendar.ubc.ca/vancouver/courses.cfm?page=code","courses.cfm?",'text'):
        dataDump.append(link)
    for i in dataDump:
        print(i)
    import csv
    with open("ubccourses.csv","wb") as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(("Course Name","Course Code"))
        for i in range(1,len(dataDump)):
            if len(dataDump[i]) <= 4 and dataDump[i][-1].isupper():
                writer.writerow((dataDump[i+1], dataDump[i]))


def writeSubjectCourseCodes(subjectCodes = False, mode = 'csv'):

    # This writes all the subject codes to a csv file

    import csv      # For writing subject codes into a file
    import time     # To slow down requests to server
    import random

    # I'm too lazy to implement alternate mechanisms
    if mode != 'csv':
        return

    else:
        
        # If no subject code is provided, get all of them from the source CSV - "ubccourses.csv"

        if not subjectCodes:
            sourceCSV = 'ubccourses.csv'

            with open(sourceCSV,"rb") as csvfile:
                reader = csv.reader(csvfile,delimiter=",")
                
                for row in reader: 
                    subjectCode = row[1]
                    if len(subjectCode) in range(2,5):
                        courseCodes = getCourseCodes(subjectCode)

                        fileName = "./subjectClasses/" + subjectCode + ".csv"

                        with open(fileName,"wb") as csvfilex:

                            writer = csv.writer(csvfilex, delimiter=',')
                            writer.writerow(("Course Code",""))
                            for courseCode in courseCodes:
                                writer.writerow((courseCode,""))

                        time.sleep(random.uniform(0.5,1.0))


        elif subjectCodes:
            if isinstance(subjectCodes, basestring):
                courseCodes = getCourseCodes(subjectCodes)

                fileName = "./subjectClasses/" + subjectCodes + ".csv"

                with open(fileName,"wb") as csvfile:

                    writer = csv.writer(csvfile, delimiter=',')
                    writer.writerow(("Course Code",""))
                    for courseCode in courseCodes:
                        writer.writerow((courseCode,""))
            else:
                for subjectCode in subjectCodes:

                    courseCodes = getCourseCodes(subjectCode)

                    fileName = "./subjectClasses/" + subjectCode + ".csv"

                    with open(fileName,"wb") as csvfile:

                        writer = csv.writer(csvfile, delimiter=',')
                        writer.writerow(("Course Code",""))
                        for courseCode in courseCodes:
                            writer.writerow((courseCode,""))

                    time.sleep(random.uniform(0.5,1.0))

def writeCourseInfo():

    import os       # For listing files
    import csv      # For writing to csv
    import time     # For sleeping - avoid excessive server pings
    import random   # Chooses random time

    # Gets course codes from subject files
    files = os.listdir("./subjectClasses/")

    # Fetches course codes from CSV files
    for filex in files:
        courseCodes = []

        fileName = "./subjectClasses/" + filex

        with open(fileName,"rb") as csvfile:
            reader = csv.reader(csvfile,delimiter=",")

            for row in reader:
                courseCodes.append(row[0])
        courseCodes = courseCodes[1:]
        
    # Gets department from file name
        department = filex.split(".")[0]

    # Gets course information from course codes
        with open(fileName,"wb") as csvfile:
            # Opens reader, writes header
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(("Course Title","Course Code", "Course Link", "Course Description", "Course Credits", "Course Pre-Reqs"))

            for courseCode in courseCodes:
                courseInfo = getCourseInformation(department, courseCode)
                
                if courseInfo.prereqs:
                    prereqs = courseInfo.prereqs
                else:
                    prereqs = str(courseInfo.prereqs)

                print(department + ", " + courseInfo.code)
                row = (courseInfo.title, courseInfo.code, courseInfo.link, courseInfo.description, courseInfo.credits, prereqs)
                row = [s.encode('utf-8') for s in row]
                writer.writerow(row)

            time.sleep(random.uniform(1.0,10.0))

def splitCourses(preReqs):
    for i in range(1,len(preReqs)):
        if preReqs[i-1].islower() and preReqs[i].isupper():
            preReqs = preReqs[:i] + ", " + preReqs[i:]
        if preReqs[i-1] == "," and preReqs[i] != ' ':
            preReqs = preReqs[:i] + " " + preReqs[i:]
    return preReqs
    
def checkCSV():
    import os       # For listing files
    import csv      # For writing to csv

    # Gets course codes from subject files
    files = os.listdir("./subjectClasses/")
    

    # Fetches course codes from CSV files
    for filex in files:
        fileName = "./subjectClasses/" + filex
        with open(fileName,"rb") as csvfile:
            reader = csv.reader(csvfile,delimiter=",")

            for row in reader:
                preReqs = row[5]
                if preReqs != 'False' and preReqs != 'Course Pre-Reqs':
                    preReqsx = splitCourses(preReqs)
                    if preReqsx != preReqs and "Three" in preReqs:
                        print(filex + ' ' + row[1] + ' ' + preReqsx)
               
                    
    return


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))