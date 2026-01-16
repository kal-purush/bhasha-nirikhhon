from urllib.request import urlopen
from bs4 import BeautifulSoup
import csv
import re

csvFile = open("tree.csv","wt")


def getschool(subjectno):
   url = "http://www.dezhi.com/knowledge/subject_"+str(subjectno)

   print(url)
   html = urlopen(url)
   bs = BeautifulSoup(html.read())   
   try:
      kp=bs.find("h4",{"class":"Dz_texta_c"})
      kp_name=kp.get_text()
      
      li1 = bs.findAll("li",{"class":"li1"})
      writer = csv.writer(csvFile);
      alist=[]
      for l in li1:
         p = l.find("a")
         pname=p.get_text()
         pid=l['id'][2:]
         ppid=l['parentid'][2:]
         
         parent_knowlege=[pname,pid,ppid,subjectno,kp_name]
         #pk = parent_knowlege.split(',')
         alist.append(parent_knowlege)
         #print(parent_knowlege)
         
         l3 = l.findAll("li",{"class":"li3"})    
         for i3 in l3:
             txt = i3.find("a").get_text()
             cid=i3['id'][2:]
             cpid=i3['parentid'][2:]
             child_knowlege=[txt,cid,cpid,subjectno,kp_name]
             alist.append(child_knowlege)
          #   print(child_knowlege)
      for item in alist:
         writer.writerow(item)
   finally:
  
      print('exec over')
   return


for num in range(1,20):
      getschool(num)





