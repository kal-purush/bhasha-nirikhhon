#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      ELCOT
#
# Created:     26-03-2018
# Copyright:   (c) ELCOT 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------

def main():
    pass

if __name__ == '__main__':
    main()
n=int(input("enter the no:"))
temp=n
rev=0
while(n>0):
    dig=n%10
    rev=rev*10+dig
    n=n//10
if(temp==rev):
    print("yes")
else:
    print("no")