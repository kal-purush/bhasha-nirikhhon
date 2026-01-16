"""This program takes an argument -- a single url -- from the command line
   and saves the webpage as file with the same name as the page but with a
   prepended "website-saver-" (E.g., google.com/index.php is saved as 
   website-saver-index.php) in the working directory."""

import sys
import urllib.request

def getFile(url):
    """Takes url, downloads file"""
    if checkURL(url) == False:
        print("Not a valid url!")
        return
    response = urllib.request.urlopen(url)
    print("Got file!")
    html = response.read()
    filename = getFileName(url)
    saveFile(str(html), "website-saver-" + filename)

def getFileName(url):
    return url.split("/")[-1]
    
def checkURL(url):
    """Sees if url looks like a valid url. Full disclosure: really stupid
       function, I'm just writing it for the experience."""
    if url[:8] == "https://" or url[:7] == "http://":
        return True
    else:
        return False

def saveFile(data, filename):
    """File saver helper function"""
    f = open(filename, "w")
    f.write(data)
    print("Saved file!")

if __name__ == "__main__":
    """Why did the ckicekn cross the road?
       To see if she was on __main__ street! :D"""
    args = sys.argv
    url = args[1] # hopefully a url...
    getFile(url)
    print("Finished program!?")