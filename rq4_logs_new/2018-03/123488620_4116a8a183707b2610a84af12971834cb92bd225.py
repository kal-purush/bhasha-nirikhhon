#!/usr/bin/python
import os
print "Job Applicants"
f = open("lcc/jobs/applications/index.html", "r+")
for i in range(90):
    f.write("\n                   ")
f.close()
f = open("lcc/jobs/applications/index.html", "r+")
f.write("<!--#include virtual='/lcc/css/top2017.inc'-->\n")
f.write("<div class='leg_Col4of4-First'>\n")
f.write("<h2>Job Applicants</h2><br>\n")
for root, dirs, files in os.walk('lcc/jobs/applications/'):
    files.sort()
    for name in files:
        if ((name!="index.html")&(name!=".htaccess")):
            f.write("<a href='"+name+"'>"+name.rstrip(".pdf")+"</a>\n<br><br>\n")
            print name.rstrip(".pdf")
f.write("<!--#include virtual='/lcc/css/footer2012.inc'-->\n</div>\n")
f.close()
