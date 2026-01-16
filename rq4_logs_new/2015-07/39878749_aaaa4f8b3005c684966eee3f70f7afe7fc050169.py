#!/usr/bin/python3

import markdown
import sys
import codecs
import datetime
import os

##Basedir
BASEDIR="site/"


now = datetime.date.today()

articlename = sys.argv[1][0:sys.argv[1].find(".")]

template = open("template.html", "r").read()
md = codecs.open(sys.argv[1], mode="r", encoding="utf-8").read()

template = template.replace("#TITLE#", sys.argv[2])
template = template.replace("#CONTENT#", markdown.markdown(md, output_format="html5"))

path = BASEDIR+"/"+str(now.year)+"/"+str(now.month)+"/"

os.makedirs(path, exist_ok=True)

output = codecs.open(path+articlename+".html", "w", encoding="utf-8", errors="xmlcharrefreplace")

output.write(template)

output.close()

