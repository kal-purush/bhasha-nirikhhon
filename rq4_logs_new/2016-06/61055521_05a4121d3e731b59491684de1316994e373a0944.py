import requests
import json
import calendar
import os.path
from datetime import datetime, timedelta

#access token an be retrieved from https://api.slack.com/docs/oauth-test-tokens
_token = "ACESS TOKEN HERE"
_domain = "DOMAIN HERE" #if your slack is oneset.slack.com, _domain = "oneset"

#arbitrary page num. Starting from the end of the pages is required since the content on each page is different after deletions 
#(deleting stuff on page 1 will make stuff on page 2 go to page 1. If we start at the end, we dont have this problem)
pageNum = 100

#totals
totalSize = 0
totalFreed = 0

#file types dict and count for amount of files
fileTypes = dict()
count = 0
deletedCount = 0

if __name__ == '__main__':
    while pageNum > 0:
        print "PAGE NUM: " + str(pageNum)
        files_list_url = 'https://slack.com/api/files.list'
        data = {"token": _token, "page": pageNum}
        response = requests.post(files_list_url, data = data)
        #if no files on this page, skip it
        if len(response.json()["files"]) == 0:
            print "SKIPPING PAGE"
            pageNum -= 1
            continue

        for f in response.json()["files"]:
            #get the extension of the current file
            extension = os.path.splitext(f["name"])[1]
            count += 1
            #if the extension is already tracked, add its size, if its not, create a new dictionary entry
            if extension in fileTypes:
                #exists
                fileTypes[extension] += f["size"]
            else:
                #doesnt exist
                fileTypes[extension] = f["size"]

            #Add types for deletion (FILES WITH THESE EXTENSIONS WILL BE DELETED FROM YOUR SLACK)
            if extension == ".mp4" or extension == ".apk" or extension == ".mov" or extension == ".avi" or extension == ".george":
                deletedCount += 1
                #add the freed size
                totalFreed += f["size"]
                print "Deleting file " + f["name"] + "..."
                timestamp = str(calendar.timegm(datetime.now().utctimetuple()))
                #post the delete call
                delete_url = "https://" + _domain + ".slack.com/api/files.delete?t=" + timestamp
                # result = requests.post(delete_url, data = {
                #     "token": _token, 
                #     "file": f["id"], 
                #     "set_active": "true", 
                #     "_attempts": "1"})
                #print the result of the call
                #print result.json()
        #go to the next page
        if deletedCount == 0:
            print "No files with the desired file types on the current page"
        pageNum -= 1
    print "\n"
    print "DONE!"
    print "-----------------"

    #print each file type and its related size
    for _type in fileTypes:
        totalSize += fileTypes[_type]
        print _type + ": " + str(fileTypes[_type] / (1000000)) + "mb"

    #final print of info
    print "\n"
    print "TOTAL SIZE: " + str(totalSize / (1000000)) + " mb"
    print "TOTAL FREED: " + str(totalFreed / (1000000)) + " mb"
    print "TOTAL FILES REMAINING: " + str(count - deletedCount)
    print "TOTAL FILES: " + str(count)
    print "TOTAL FILES DELETED: " + str(deletedCount)