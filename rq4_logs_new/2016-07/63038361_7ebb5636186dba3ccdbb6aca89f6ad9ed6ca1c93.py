import requests
import simplejson
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("repoName",type=str,help="The name of the new GitHub Repository")
parser.add_argument("repoDescription",type=str,help="A description for the new GitHub Repository")
parser.add_argument("-t", "--token",type=str,help="The GitHub User's Token. If no token is provided then an attempt at pulling the value from ")
args = parser.parse_args()

repoName = args.repoName
if args.token is not None:
    t=args.token
else:
    t = os.environ['gitT'] 

r = requests.post('https://api.github.com/user/repos?access_token='+t, json = {"name":args.repoName,"description":args.repoDescription,"auto_init":True})
c = r.content
j = simplejson.loads(c)
idUrl = {}

for item in j.items():
    if item[0]=="id":
        idUrl[item[0]] = item[1]
    elif item[0]=="git_url":
        idUrl[item[0]] = item[1]

if len(idUrl) > 0:
    print ("{}".format(str(idUrl)))
else:
    print ("Error: Please review the response \n {}".format(j))