import os
import re
import zipfile
import sys
import json
class JD:
    def __init__(self, jd_path):
        sys.stderr.write(jd_path + '\n')
        self.jd_path = jd_path

    def getDownloadLinks(self):
        cfgPath = os.path.join(self.jd_path, 'cfg')
        latestZipFile = None
        latestZipOrdinal = -1
        #Find the latest zip file containing the download list
        for file in os.listdir(cfgPath):
            if match := re.match(r"downloadList(\d+)\.zip", file):
                ordinal = int(match.group(1))
                if ordinal > latestZipOrdinal:
                    latestZipFile = file
                    latestZipOrdinal = ordinal

        if latestZipFile == None:
            sys.stderr.write("Couldn't find latest downloadZip in " + cfgPath)
            sys.exit(-1)
        #print(latestZipFile)
        with zipfile.ZipFile(os.path.join(cfgPath, latestZipFile), mode='r') as linkZip:
            ret = {}
            for entry in linkZip.namelist():
                #If this is a package.  We can skip these
                if match := re.match(r"^(\d+)$", entry):
                    continue
                #This is a link
                if match := re.match(r"^(\d+)_(\d+)$", entry):
                    linkData = json.loads(linkZip.read(entry))
                    #Skip disabled links
                    if not linkData.get('enabled'):
                        continue
                    url = linkData.get('url')
                    status = linkData.get('finalLinkState')
                    size = linkData.get('size')
                    ret[str(url)] = { 'size': int(size), 'status': status }

            return ret




if __name__ == '__main__':
    #Test case
    jd = JD(os.path.join(os.environ["HOME"],  'docker/jdownloader/config'))
    print(jd.getDownloadLinks())