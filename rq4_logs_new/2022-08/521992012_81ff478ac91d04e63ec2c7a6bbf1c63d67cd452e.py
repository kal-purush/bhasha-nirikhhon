import sys #Allows to kill program if wrong inpput
from colorama import * #Allows custom text color in terminal
import time
import random
import string
import datetime


print("""\


  /$$$$$$  /$$$$$$$$       /$$                 /$$                       /$$               /$$                    
 /$$__  $$|_____ $$/      |__/                | $$                      | $$              |__/                    
|__/  \ $$     /$$/        /$$ /$$$$$$$   /$$$$$$$ /$$   /$$  /$$$$$$$ /$$$$$$    /$$$$$$  /$$  /$$$$$$   /$$$$$$$
  /$$$$$$/    /$$/        | $$| $$__  $$ /$$__  $$| $$  | $$ /$$_____/|_  $$_/   /$$__  $$| $$ /$$__  $$ /$$_____/
 /$$____/    /$$/         | $$| $$  \ $$| $$  | $$| $$  | $$|  $$$$$$   | $$    | $$  \__/| $$| $$$$$$$$|  $$$$$$ 
| $$        /$$/          | $$| $$  | $$| $$  | $$| $$  | $$ \____  $$  | $$ /$$| $$      | $$| $$_____/ \____  $$
| $$$$$$$$ /$$/           | $$| $$  | $$|  $$$$$$$|  $$$$$$/ /$$$$$$$/  |  $$$$/| $$      | $$|  $$$$$$$ /$$$$$$$/
|________/|__/            |__/|__/  |__/ \_______/ \______/ |_______/    \___/  |__/      |__/ \_______/|_______/                                                                                                                                                                                                                                                                                                                                                   
               """)



#"L" is code
y = input(Fore.RED +"To access this program, you may have a code provided in the message we gave you, as well as the appointment code we may need later. Please put in the code " + Fore.RESET)
if (y == "ENTER") or "enter" or "Enter":
    print(Fore.BLUE + "Loading.....")
else: sys.exit()
def countdown(time_sec):
    while time_sec:
        mins, secs = divmod(time_sec, 60)
        timeformat = '{:02d}:{:02d}'.format(mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        time_sec -= 1
countdown(1)

def get_random_string(length):
    letters = string.ascii_uppercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    print("Your unique reference code", "is:", result_str)

get_random_string(6)
countdown(1)

name = input("Insert patient name " )
if name == "Ray": 
    print("  ")
else:
    print("You have put in the wrong name")
    sys.exit()
age = input("Please put in patient age here:" )
patient_status= input("Put in status: New or Old? ")
condition = "new" in patient_status.lower()
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
Day = datetime.date(2022, 11, 27) - datetime.date.today() 
Day = str(Day)
print("Your appointment will be in " + Day.strip("0: ,"))
#2nd code is "ABC"
x = input("Do you have a second code that was provided? ")
if "yes" in x.lower():
 print("You may proceed")
else: 
    print("""\
        
 /$$   /$$                                                               /$$                                 /$$                     /$$
| $$  | $$                                                              |__/                                | $$                    | $$
| $$  | $$  /$$$$$$  /$$    /$$ /$$$$$$         /$$$$$$        /$$$$$$$  /$$  /$$$$$$$  /$$$$$$         /$$$$$$$  /$$$$$$  /$$   /$$| $$
| $$$$$$$$ |____  $$|  $$  /$$//$$__  $$       |____  $$      | $$__  $$| $$ /$$_____/ /$$__  $$       /$$__  $$ |____  $$| $$  | $$| $$
| $$__  $$  /$$$$$$$ \  $$/$$/| $$$$$$$$        /$$$$$$$      | $$  \ $$| $$| $$      | $$$$$$$$      | $$  | $$  /$$$$$$$| $$  | $$|__/
| $$  | $$ /$$__  $$  \  $$$/ | $$_____/       /$$__  $$      | $$  | $$| $$| $$      | $$_____/      | $$  | $$ /$$__  $$| $$  | $$    
| $$  | $$|  $$$$$$$   \  $/  |  $$$$$$$      |  $$$$$$$      | $$  | $$| $$|  $$$$$$$|  $$$$$$$      |  $$$$$$$|  $$$$$$$|  $$$$$$$ /$$
|__/  |__/ \_______/    \_/    \_______/       \_______/      |__/  |__/|__/ \_______/ \_______/       \_______/ \_______/ \____  $$|__/
                                                                                                                           /$$  | $$    
                                                                                                                          |  $$$$$$/    
                                                                                                                           \______/     
                                               """) + sys.exit
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
countdown(1)
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
Details = (age, name, patient_status,"Appointment in:" + Day2.strip("0: ,"))
# Note: if **input** won't ever work as thats a function, i named the variable "a" , hence "if a=..."
a = input("do you want ur Details? (Input Yes or No) ")
if a == "Yes": 
    print(Details)
else : 
    print("""\
        
 /$$   /$$                                                               /$$                                 /$$                     /$$
| $$  | $$                                                              |__/                                | $$                    | $$
| $$  | $$  /$$$$$$  /$$    /$$ /$$$$$$         /$$$$$$        /$$$$$$$  /$$  /$$$$$$$  /$$$$$$         /$$$$$$$  /$$$$$$  /$$   /$$| $$
| $$$$$$$$ |____  $$|  $$  /$$//$$__  $$       |____  $$      | $$__  $$| $$ /$$_____/ /$$__  $$       /$$__  $$ |____  $$| $$  | $$| $$
| $$__  $$  /$$$$$$$ \  $$/$$/| $$$$$$$$        /$$$$$$$      | $$  \ $$| $$| $$      | $$$$$$$$      | $$  | $$  /$$$$$$$| $$  | $$|__/
| $$  | $$ /$$__  $$  \  $$$/ | $$_____/       /$$__  $$      | $$  | $$| $$| $$      | $$_____/      | $$  | $$ /$$__  $$| $$  | $$    
| $$  | $$|  $$$$$$$   \  $/  |  $$$$$$$      |  $$$$$$$      | $$  | $$| $$|  $$$$$$$|  $$$$$$$      |  $$$$$$$|  $$$$$$$|  $$$$$$$ /$$
|__/  |__/ \_______/    \_/    \_______/       \_______/      |__/  |__/|__/ \_______/ \_______/       \_______/ \_______/ \____  $$|__/
                                                                                                                           /$$  | $$    
                                                                                                                          |  $$$$$$/    
                                                                                                                           \______/     
                                               """)