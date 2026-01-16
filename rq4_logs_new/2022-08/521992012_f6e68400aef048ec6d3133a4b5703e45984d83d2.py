import sys #Allows to kill program if wrong program
from colorama import * #Allows custom text color in terminal
import time

#"Z" is code
y = input(Fore.BLUE +"To access this program, you may have a code provided in the message we gave you, as well as the appointment code we may need later. Please put in the code ")
if (y == "Z") or "z":
    print("Loading.....")
else: sys.exit()
def countdown(time_sec):
    while time_sec:
        mins, secs = divmod(time_sec, 60)
        timeformat = '{:02d}:{:02d}'.format(mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        time_sec -= 1
countdown(2)
name = input("Insert patient name " )
if name == "Ray": 
    print("  ")
else:
    print("You have put in the wrong name")
    sys.exit()
age = input("Please put in patient age here:" )
patient_status= input("Put in status: New or Old? ")
condition = "New" or "new" in patient_status
if condition: 
    print("You are a NEW patient!")
else: 
    print("You are an OLD patient" + Fore.RESET) 
#The code is "XYZ" , thats pretty much it lol ty :D
Date = input("Put in your unique access code " )
if Date == "XYZ":
    print("Your appointment date is on 27/11/24",)
else:
    print("Your code is incorrect, Maybe you put in another code by accident?",)
    sys.exit()

    
#Date sustem which calcuates appointment dates
import datetime
Day = datetime.date(2022, 11, 27) - datetime.date.today() 
Day = str(Day)
print("Your appointment will be in " + Day.strip("0: ,"))
#2nd code is "ABC"
x = input("Do you have a second code that was provided? ")
if (x == "Yes") or "yes" or "YES":
 print("You may proceed")
else: print("Have a nice day") + sys.exit()
print("You may request a new code that may speed up your date, in such case please insert the new code here")
print("ㅤㅤㅤㅤㅤㅤ")
print("Please wait for a moment before putting in a new code")
print("ㅤㅤㅤㅤㅤㅤ")
#Time library that counts down after putting in code:
def countdown(time_sec):
    while time_sec:
        mins, secs = divmod(time_sec, 60)
        timeformat = '{:02d}:{:02d}'.format(mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        time_sec -= 1
countdown(5)
Fore.RESET
Date2 = input(Fore.CYAN + "Put in the new code: ")
if Date2 == "ABC":
    print("Your appointment is on 17/8/22")
else:
        print("The code you put in was icorrect.")
        sys.exit()
Day2 = datetime.date(2022, 8, 17) - datetime.date.today() 
Day2 = str(Day2)
print("Your appointment will be in " + Day2.strip("0: ,"))
Details = (age, name, patient_status,"Appointment in:" Day2.strip("0: ,"))
a = input("do you want ur Details? (Input Yes or No) ")
if a == "Yes": 
    print(Details)
else : print("have a nice day :)" + Fore.RESET)