import json
import re
import os
import time
import datetime

def divideCommentsMonth(repo,endYear):
    repos = repo.split(sep="/")
    folder = repos[1]
    cDir = "public/data/" + repo+ "/" + "comments/"

    files = os.listdir(cDir)
    # print(files)
    pages=len(files)
    print(pages)
    comments_created = {}

    for page in range(1,pages+1):
        print(page)
        with open(cDir + "allComments-"+str(page)+".json",'r') as f:
            data = json.loads(f.read())

            for item in data:
                date = item["created_at"];
                date=date[0:7]

                if (date not in comments_created):
                    comments_created[date] = []
                comments_created[date].append(item)


    for year in range(2008,int(endYear) + 1):
        for month in range(1,13):
            date = "%d-%02d" %(year,month)
            if( date not in comments_created):
                with open(cDir + date + ".json",'w') as f:
                    json.dump({},f)
            else :
                with open(cDir + date + ".json",'w') as f:
                    json.dump(comments_created[date],f)