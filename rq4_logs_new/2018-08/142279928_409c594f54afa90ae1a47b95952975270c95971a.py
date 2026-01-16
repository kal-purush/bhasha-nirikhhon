import json
import re
import os
import time
import datetime
def divideIssueMonth(repo):
    repos = repo.split(sep="/")
    folder = repos[1]
    issuesDir = "F:/Github/githubAnalysis-master (2)/githubAnalysis-master/public/" + folder + "/" + "dealdIssues/"
    files = os.listdir(issuesDir)
    # print(files)
    pages=len(files)
    print(pages)


    for page in range(1,pages):
        with open(issuesDir + "issues-"+str(page)+".json",'r') as f:
            data = json.loads(f.read())
            print(data)
            for item in data:
                date = item["created_at"];
                newdate=date[0:7]
                newpath = r"F:/Github/githubAnalysis-master (2)/githubAnalysis-master/public/{}/newIssues".format(folder)
                if not os.path.exists(newpath):
                    os.makedirs(newpath)
                with open("F:/Github/githubAnalysis-master (2)/githubAnalysis-master/public/"+folder + "/" + "newIssues/" + str(newdate)+".json", "w",newline="") as f:
                    json.dump(item, f)

repos = ["d3/d3","airbnb/javascript","nlohmann/json"]
for item in repos:
    divideIssueMonth(item)