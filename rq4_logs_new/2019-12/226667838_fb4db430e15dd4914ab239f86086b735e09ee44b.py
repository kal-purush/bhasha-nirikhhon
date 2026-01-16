import os, glob, time, operator
from datetime import timedelta,date
import datetime,shutil

def modification_date(filename):
    t = os.path.getmtime(filename)
    modifiedDate=str(datetime.datetime.fromtimestamp(t))
    modifiedDate=modifiedDate[0:4]+'_'+modifiedDate[5:7]+'_'+modifiedDate[11:13]
    return  modifiedDate

###245 GB=263066746880 Byte
###500 MB=524288000 Byte
def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.3f %s" % (num, x)
        num /= 1024.0

def GetFileSize(filepath):
    fileSize=os.stat(filepath).st_size
    return fileSize

def getDates():
	days=[]
	today = date.today()  ###will be in format YYYY-MM-DD
	for i in range (30):
		day="day"+str(i)
		day=today-timedelta(days=int(i))
		day=str(day).split('-')
		day= day[0] + '_' +day[1] + '_' + day[2]
		days.append(day)
	return days


def get_oldest_file(files, _invert=False):
    """ Find and return the oldest file of input file names.
    Only one wins tie. Values based on time distance from present.
    Use of `_invert` inverts logic to make this a youngest routine,
    to be used more clearly via `get_youngest_file`.
    """
    gt = operator.lt if _invert else operator.gt
    # Check for empty list.
    if not files:
        return None
    # Raw epoch distance.
    now = time.time()
    # Select first as arbitrary sentinel file, storing name and age.
    oldest = files[0], now - os.path.getctime(files[0])
    # Iterate over all remaining files.
    for f in files[1:]:
        age = now - os.path.getctime(f)
        if gt(age, oldest[1]):
            # Set new oldest.
            oldest = f, age
    # Return just the name of oldest file.
    return oldest[0]


backupPath=r"D:\laxisilon.pftla.local\Clear\ifs\PRODUCTION_BACKUPS\SQLDB\Full_Production_DB_Backup_Products"
#backupPath1=backupPath

for roots, dirs, files in os.walk(backupPath):
    for dir in dirs:
        backupPath1= backupPath
        backupPath1+="\\"+dir
        fileSizeMoved=0
        filesToMove=[]
        noOfFilesToMove=0
        fileSizeToMoveInByte=0
        days=getDates()
        startDate=0
        endDate=0
        condition=True
        for roots,dirs,files in os.walk(backupPath1):
           for i in range(32):
                files = glob.glob(backupPath1+'\\*.bak')
                oldestFile=get_oldest_file(files)
                directoryNameLength=len(dir)
                tempFileName=oldestFile[95+directoryNameLength::]
                moveToTemp=r"D:\filesToMoveToTN\\"+dir
                if(os.path.isdir(moveToTemp)==False):
                    os.mkdir(moveToTemp)
                modificationDate=modification_date(oldestFile)
                if( modificationDate not in(days)and noOfFilesToMove<32 and fileSizeToMoveInByte<=524288000):
                    filesToMove.append(oldestFile)
                    fileSizeToMoveInByte+=GetFileSize(oldestFile)
                    noOfFilesToMove+=1
                    shutil.move(oldestFile,moveToTemp+"\\"+tempFileName)
                else:
                    startFile=filesToMove[0]
                    endFile=filesToMove[len(filesToMove)-1]
                    print(startFile,endFile)
                    break



