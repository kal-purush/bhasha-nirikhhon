# import all stuff
import os
import sys
import time
import datetime
import subprocess

date = ""

# check if second param is set
if (len(sys.argv) > 1):
    # date from param - format "%d/%m/%Y"
    dateParam = sys.argv[1]

    isValid = True
    try:
        day,month,year = dateParam.split("/")
        datetime.datetime(int(year), int(month), int(day))
    except ValueError:
        isValid = False    

    if isValid is False:
        print("Format must be 'DD/MM/YYYY'")
    else:
        date = time.mktime(datetime.datetime.strptime(dateParam, "%d/%m/%Y").timetuple())

class GitLog:
    hash = "COMMITHASH" + "%H"
    branch = "COMMITBRANCH" + "%d"
    commit = "COMMITCOMMENT" + "%B"
    userName = "USERNAME" + "%aN"
    userEmail = "USEREMAIL" + "%ae"
    committerDate= "COMMITDATE" + "%ct" # committer date (unix timestamp)
    gitLog = "git log --format="
    delimiter = "---DELIMITER---"

    def get(self):
        return self.gitLog + self.userName + self.userEmail + self.hash + self.commit + self.branch + self.committerDate + self.delimiter

def writeInFile(line):
    # write branch
    branch = line[(line.find('COMMITBRANCH')+13):]
    if branch: releaseNotes.write("# " + line[(line.find('COMMITBRANCH')+12):] + "\n")
    # write name + email
    releaseNotes.write("## Autor: " + line[(line.find('USERNAME')+8):line.find('USEREMAIL')] + " - " + line[(line.find('USEREMAIL')+9):line.find('COMMITHASH')] + "\n")
    # write hash
    releaseNotes.write("** Hash: " + line[(line.find('COMMITHASH')+10):line.find('COMMITCOMMENT')] + "\n")
    # write comment - splitlines function because in commit is sometimes new line 
    releaseNotes.write("** Commit: " + ' '.join(line[(line.find('COMMITCOMMENT')+13):line.find('COMMITBRANCH')].splitlines()) + "\n")
    # write commit date
    releaseNotes.write("** Date: " + time.ctime(int(line[(line.find('COMMITDATE')+10):])) + "\n")
    releaseNotes.write("\n")

# run command
cmdOutput = subprocess.getstatusoutput(GitLog().get())

# check if no error - 0 == noError
if cmdOutput[0] == 0:
    # create release notes file
    releaseNotesName = "git-logs-" + str(datetime.datetime.now().date()) + ".md"

    # check if file exists
    if not os.path.exists(releaseNotesName): 
        os.system("touch " + releaseNotesName)
    else:
        os.system("rm " + releaseNotesName)   

    # write output into release notes file
    with open(releaseNotesName, 'a') as releaseNotes:
            for commit in cmdOutput[1].split("---DELIMITER---"):
                if commit:
                    if date != "":
                        if date <= int(commit[(commit.find('COMMITDATE')+10):]):
                            writeInFile(commit)
                    else:
                        writeInFile(commit)

else:
    print("Sorry, something went wrong. " + cmdOutput[1])




