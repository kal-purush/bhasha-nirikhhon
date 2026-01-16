import random
import datetime

time=datetime.datetime.now()

def randm():
  #RAND NUM FUNC IS "random.randint(min,max)"
  print('The Lottery number is' , random.randint(100,999))
  print('Generated on', time.strftime("%d %b %Y | %H:%M"))

def question():
  msg=input("\nDo you want to try again one more time?\n")
  if msg=="Yes":
    print(randm(),question(),input())
  else:
    exit()

def inputhuy():
  Message=input("[x]====== Do you want to generate lottery number? ======[x]\n")
  if Message=="Yes":
	  print(randm(), question(), input())
  else:
    print("Incorrect message typed!")

inputhuy()