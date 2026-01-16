#!/usr/bin/env python3
'''
    Author: Nahid Sarker
    Date created: 11/11/2017
    Date modified: 11/13/2017
    Purpose: Download videos from website/s
    Created on: Lubuntu 16.04.3 LTS
    Tested on: Lubuntu 16.04.3 LTS, Windows 10

    **This in python3
'''
import requests
import random
import re
import sys
import os.path

def LoadUserAgents(uafile="user_agents.txt"):
    """
    uafile : string path to text file of user agents, one per line
    """
    uas = []
    with open(uafile, 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip()[1:-1-1])
    random.shuffle(uas)
    return uas
    
def download_file(url):
    '''
    input: url=url of webpage
    output: file downloaded
    '''
    uas = LoadUserAgents()
    ua = random.choice(uas)  # select a random user agent
    headers = {
        "Connection": "close",  # another way to cover tracks
        "User-Agent": ua}

    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url,headers=headers,stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename

def multiple_links(f_name):
    '''
    input: f_name=name of file with all of the video links
    output: all videos from link (if valid) downloaded
    '''
    with open(f_name, "r") as f:
        counter = 0
        for line in f:
            line = line.strip("\n")
            download_file(str(line))
            counter += 1
            print(str(counter) + " " + line)    #Print which file was downloaded

if(__name__ == "__main__"):
    if(len(sys.argv) < 1):  #Check if no argiements given
        print("Useage:\n")
        print("For just one link")
        print("     downloadVideos.py [http://example.com]\n")
        print("For multiple files")
        print("     downloadVideos.py [file_with_links]")
        exit(0)
    if(len(sys.argv) > 2):  #Check if too many arguements
        print("Useage:\n")
        print("For just one link")
        print("     downloadVideos.py [http://example.com]\n")
        print("For multiple files")
        print("     downloadVideos.py [file_with_links]")
        exit(0)
    
    if("http" in sys.argv[1]):  #is a website?
        download_file(sys.argv[1])
    elif(os.path.isfile(sys.argv[1])):  #Check if file exist
        multiple_links(sys.argv[1])
    else:
        print("Error\nNot a file!")
