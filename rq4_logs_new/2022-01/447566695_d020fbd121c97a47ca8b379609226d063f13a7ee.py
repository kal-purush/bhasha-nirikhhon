# Version : Beta
# Author : wallga
# Build for Python 3.x

import datetime
 
now = datetime.datetime.now()       
nowY = now.strftime("%Y")
nowM = now.strftime("%m")
nowD = now.strftime("%d")

anniv_name = input("Anniversary Name : ")
anniv = input("Anniversary Date : ") # 8 
annivY = anniv[0:4]
annivM = anniv[4:6]
annivD = anniv[6:]

nthY = int(nowY) - int(annivY)

if nowY != annivY and nowM == annivM and nowD == annivD:
    print("Today is {0}th {1}! Congratulations!".format(nthY, anniv_name))
elif nowY == annivY and nowM == annivM and nowD == annivD:
    print("Today is {0}!".format(anniv_name))
else:
    print("There are {0} days left until the anniversary.") # uncode
    ###