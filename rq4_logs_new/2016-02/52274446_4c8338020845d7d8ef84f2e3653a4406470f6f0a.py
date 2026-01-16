import os
import urllib
#this is the directory where is the log files locate,
root = "/home/mohanad"

keywords = "Windows", "Linux", "OS X", "Ubuntu", "Googlebot", "bingbot", "Android", "YandexBot", "facebookexternalhit"
d = {} 
urls = {}
total = 0
import gzip
for filename in os.listdir(root):
    if not filename.startswith("access.log"):
        print "Skipping unknown file:", filename
        continue
    if filename.endswith(".gz"):
        fh = gzip.open(os.path.join(root, filename))
    else:
        fh = open(os.path.join(root, filename))    
    print "Going to process:", filename
    for line in fh:
        total = total + 1
        try:
             source_timestamp, request, response, referrer, _, agent, _ = line.split("\"")
             method, path, protocol = request.split(" ")
             url = "http://enos.itcollege.ee" + urllib.unquote(path)
             try:
                urls[url] = urls[url] + 1
             except:
                urls[url] = 1

             for keyword in keywords:
                 if keyword in agent:
                    try:
                        d[keyword] = d[keyword] + 1
                    except KeyError:
                        d[keyword] = 1
                    break # Stop searching for other keywords
        except ValueError:
            pass # This will do nothing, needed due to syntax
 
print "Total lines:", total
 
results = urls.items()
results.sort(key = lambda item:item[1], reverse=True)
for keyword, hits in results[:5]:
    print keyword, "==>", hits, "(", hits * 100 / total, "%)"