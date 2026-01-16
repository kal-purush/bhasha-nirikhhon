#!/usr/bin/env python
import subprocess
import os
import glob
import re
import time
import sys
from subprocess import Popen
import pipes
import argparse
import time

LOCAL_REPO_DIRECTORY = '/home/steinachim/THOR/'
TAG_PREFIX = 'hector_working_version-'

def execute(action, tag_name):
    if ( action == "list" ):
      os.system("cd "+LOCAL_REPO_DIRECTORY+"src/thor_mang_scripts;git tag")
      return
    
    rosinstall_file = LOCAL_REPO_DIRECTORY + ".rosinstall"
    with open (rosinstall_file, "r") as myfile:
        data=myfile.read()
    allFolders = re.findall("local-name: \S*", data)   
    allUris = re.findall("uri: \S*", data)
    
    result = 0
    for i in range(len(allFolders)):            
        folders = re.findall("\S*",allFolders[i])            
        local_folder = LOCAL_REPO_DIRECTORY + folders[2].replace(',','').replace(' ','')
        
        if(os.path.isdir(local_folder)):                          
            print "Now processing "+local_folder+" folder...."
            if ( action=="tag" ):
              result = result | os.system("cd "+local_folder+";git tag "+tag_name+"; cd  "+LOCAL_REPO_DIRECTORY)
            elif ( action=="checkout" ):
              if not TAG_PREFIX in tag_name:
                # assume existing branch, dangerous
                result = result | os.system("cd "+local_folder+";git checkout "+tag_name+"; cd  "+LOCAL_REPO_DIRECTORY)
              else:
                # assume new tag
                result = result | os.system("cd "+local_folder+";git checkout -b "+tag_name+"; cd  "+LOCAL_REPO_DIRECTORY)
            elif ( action=="diff" ):
                result = result | os.system("cd "+local_folder+";git diff tags/"+tag_name+"; cd  "+LOCAL_REPO_DIRECTORY)
            else:
                print("Error: invalid action '"+action)
                
    if (result != 0):
        print "Failure to tag some repos" 
                    
if __name__ == "__main__":
    action = "tag"
    
    current_time = time.strftime("%Y-%m-%d_%H-%M-%Z");
    tag_name = TAG_PREFIX+current_time
     
    if ( len(sys.argv) > 1 ):
      action=sys.argv[1]
    
      if ( action == "checkout" or action == "diff"):
        tag_name=sys.argv[2]
      
    execute(action, tag_name)
    