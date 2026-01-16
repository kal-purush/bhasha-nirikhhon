#!/usr/bin/python3
import sys              # access command line from py script
import os               # allow access to directories
import pandas as pd

#find disk space used by above dirs
def get_size(path):
        total = 0
        for entry in os.scandir(path):
                try:
                        if (entry.is_dir(follow_symlinks=False)): 
                                total += get_size(entry.path)
                        else
                                total += entry.stat(follow_symlinks=False).st_size
                except Exception as e:
                        print ("Exception: ", e)
                        total+=0
         return total
                        
if __name__ == '__main__':
        path = '/home'
        print ("Total arguments passed: ", len(sys.argv))
        
        directory = sys.argv[1] if len(sys.argv) >= 2 else path
        
        usage = []
        paths = []
        
        #Iterate through sub-directories
        for entry in os.scandir(directory):
                print (entry.path)
                if (entry.is_dir(follow_symlinks=False)):
                        # print (entry.path + " is a directory")
                        # print (get_size(entry.path))
                        total = get_size(entry.path)
                        print (total)
                        paths.append(entry.path)
                        usage.append(total)
                usage_dict = {'directory' : paths, 'usage' : usage}
                
                #create pandas dataframe using the dictionary created above
                df = pd.DataFrame(usage_dict)
                print(df)
                df.to_csv("disk_home_usage.csv")