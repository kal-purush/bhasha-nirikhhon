import db
import os
import time
import tabulate
from datetime import datetime
from colorama import Fore, Style, init

init() #Initialise Colorama Module

#TO-DO:
# Edit Location


##=======[GLOBAL VARIABLES]=========##
uid = ''
userInfo = ()
userType = ''


##=======[TABULATION HEADERS]=========##
car_headers = ['ID','Brand','Model','Type','Fuel','Transmission','Seating','Partner','Rate per hour']
user_headers = [' ','UserID','Username','Name','Location','Status','Total Rentals','Total Amount Spent']
partner_headers = [' ','UserID','Username','Name','Location','Total Cars','Total Rentals','Total Earnings']
partner_car_headers = ['ID','Brand','Model','Type','Fuel','Transmission','Seating','Rate per hour','Status']


##=======[DATA]=========##
car_type = {1:'Sedan',2: 'Hatchback',3: 'SUV',4: 'MUV',5: 'Coupe',6: 'Sports'}   
car_fuel = {1:'Diesel',2: 'Petrol',3: 'Electric'}                           
car_transmission = {1:'Manual',2: 'Automatic'}    
brand = {1:'Audi',2: 'BMW',3: 'Chevrolet',4: 'Ferrari',5: 'Ford',6: 'Koenigsegg',7: 'Lamborghini',8: 'Mahindra',9: 'Mazda',10: 'McLaren',11: 'Nissan',12: 'Porsche',13: 'Toyota',14: 'Volvo'}
utypes = {1:'User',2: 'Partner',3: 'Admin'}   
locations = {1:'Coimbatore',2:'Chennai',3:'Trichy'}                                          


##=======[USER FUNCTIONS]=========##
def userYourAccount():
  ### Shows User Account Info ###
  ### Name, Username, Type, Location, Stats ###
  
  global uid
  global userType
  global userInfo
  
  name = userInfo[4]
  uname = userInfo[2]
  location = userInfo[5]
  
  rentalDetails = db.getUserHistory(uid)
  rentalAmounts = db.getUserAmounts(uid)
  rentalCount = db.getUserRentals(uid)
  
  Banner()
  print(Style.BRIGHT +Fore.GREEN+"> Your Account:\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Name : "+name)
  print(Style.BRIGHT +Fore.CYAN+"[+] Username : "+uname)
  print(Style.BRIGHT +Fore.CYAN+"[+] Account Type : "+userType.title())
  print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+location)
  
  print(Style.BRIGHT +Fore.GREEN+"\n> Statistics:")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Total Cars Rented : "+str(len(rentalDetails)))
  print(Style.BRIGHT +Fore.CYAN+"[+] Cars Returned : "+str(rentalCount[0]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Cars Pending return : "+str(rentalCount[1]))
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Total Amount Paid : ₹"+format(int(rentalAmounts[0]), ',d'))
  
  a = input(Style.DIM+Fore.CYAN +"\n> Press Enter to go back")
  menu()

def userReturnCar():
  ### Shows tabulation of cars yet to return ###
  ### Brand, Model, Rental Date, Due Date, Rent, Penalty, Total ###
  
  cars = db.getUserNotReturned(uid)
  cars_headers = ['ID','Brand','Model','Rented On','Rent End','Rent','Penalty','Total']
  carsdata = []
  for x in cars:
    datebegin = datetime.fromtimestamp(int(x[3]))
    dateend = datetime.fromtimestamp(int(x[4]))
    penaltydaydifference = (dateend - datetime.now()).days
    daydifference = ((datetime.now() - datebegin).total_seconds())/3600
    penalty = 0
    price=0
    if penaltydaydifference < 0:
      penaltydays = abs(penaltydaydifference + 1)
      penalty=850*penaltydays
    
    price=round(x[6]*daydifference)
    carInfo = db.getCarInfo(x[1])[0]
    carsdata.append([cars.index(x),carInfo[1],carInfo[2],datebegin.strftime('%d %B %Y'),dateend.strftime('%d %B %Y'),"₹"+str(format(price, ',d')),"₹"+str(penalty),"₹"+str(format(price+penalty, ',d'))])
  
  Banner()
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Select Car to Return:\n")
  print(tabulate.tabulate(carsdata, headers=cars_headers, tablefmt="rounded_grid", disable_numparse=True))
  
  
  print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Rent it')
  print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
  a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
  if a.lower() == 'x':
    menu()
  else:
    userReturnConfirmation(cars[int(a)],carsdata[int(a)])
    
def userReturnConfirmation(car,data):
  ### Shows Car Details and Asks for Confirmation ###
  ### Car Name, Rental Date, Due Date, No. of delayed days, Fare, Penalty, Total ###
  
  carInfo = db.getCarInfo(car[1])[0]
  price = data[5]
  penalty = data[6]
  total = data[7]
  dateend = datetime.fromtimestamp(int(car[4]))
  penaltydaydifference = (dateend - datetime.now()).days
  penaltydays = 0
  if penaltydaydifference < 0:
      penaltydays = abs(penaltydaydifference + 1)
  Banner()
  print(Style.BRIGHT +Fore.CYAN+"> Details:\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Car : "+carInfo[1]+" "+carInfo[2])
  print(Style.BRIGHT +Fore.CYAN+"[+] Date of Rental : "+datetime.fromtimestamp(int(car[3])).strftime('%d %B %Y'))
  print(Style.BRIGHT +Fore.CYAN+"[+] Date of Return : "+datetime.fromtimestamp(int(car[4])).strftime('%d %B %Y'))
  print(Style.BRIGHT +Fore.CYAN+"[+] Delayed Days : "+str(penaltydays))
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Fare    : "+str(price))
  print(Style.BRIGHT +Fore.CYAN+"[+] Penalty : "+str(penalty))
  print(Style.BRIGHT +Fore.CYAN+"[+] Total   : "+str(total))
  
  a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to return? (Y/N) : ')
  if a.lower() == 'y':
    db.returnCar(car[1],uid,car[2],price.replace('₹','').replace(',',''),penalty.replace('₹','').replace(',',''))
    print(Fore.GREEN+"\n ==> Return Success!\n")
    time.sleep(1)
    userReturnCar()
  else:
      print(Fore.RED+"Rent Cancelled!\n")
      time.sleep(1)
      userReturnCar()
  
def userRentalHistory():
  ### Shows the list of cars rented by the user ###
  ### Brand, Model, Rental Date, Duration of rental, Price, Status[Returned / Not Returned] ###
  
  history = db.getUserHistory(uid)
  history_headers = ['ID','Brand','Model','Rented On','Duration','Price','Status']
  hisdata = []
  for x in history:
    datebegin = datetime.fromtimestamp(int(x[3]))
    daydifference = ((datetime.now() - datebegin).total_seconds())/3600
    price=round(x[6]*daydifference)
    date = datetime.fromtimestamp(int(x[3])).strftime('%d %B %Y')
    if x[9] == '0':
      status = Fore.RED+'Not Returned'+Style.BRIGHT+Fore.CYAN
    elif x[9] == '1':
      status = Fore.GREEN+'Returned'+Style.BRIGHT+Fore.CYAN
    carInfo = db.getCarInfo(x[1])[0]
    hisdata.append([history.index(x),carInfo[1],carInfo[2],date,x[5]+" Hours","₹"+str(format(price, ',d')),status])
  Banner()
  
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Your Rental History:\n")
  print(tabulate.tabulate(hisdata, headers=history_headers, tablefmt="rounded_grid", disable_numparse=True))
  
  a = input(Style.BRIGHT +Fore.CYAN +"\n> Press Enter to go back")
  menu()

def userRentCar(carInfo):
  ### Shows car details and ask for confirmation for renting the car ###
  ### Car Details -> Confirmation -> Duration -> Confirmation with price -> Add to DB ###
  
  Banner()
  print(Style.BRIGHT +Fore.CYAN+"> Details:\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Brand : "+carInfo[1])
  print(Style.BRIGHT +Fore.CYAN+"[+] Model : "+carInfo[2])
  print(Style.BRIGHT +Fore.CYAN+"[+] Type : "+carInfo[3])
  print(Style.BRIGHT +Fore.CYAN+"[+] Fuel : "+carInfo[4])
  print(Style.BRIGHT +Fore.CYAN+"[+] Transmission : "+carInfo[6])
  print(Style.BRIGHT +Fore.CYAN+"[+] Rate : ₹"+str(carInfo[7])+" per hour")
  print(Style.BRIGHT +Fore.CYAN+"[+] Partner : "+db.getPartnerInfo(carInfo[8])[0])
  print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+carInfo[9])
  
  selectedrent = input(Style.BRIGHT +Fore.GREEN +"\n> Do you want to rent this car? (Y/N) : ")
  if selectedrent.lower() == 'y':
    try:
      rentduration = int(input(Style.BRIGHT +Fore.CYAN +"\n> Enter Duration of Rent in hours (Max. 240) : "))
      if rentduration > 240:
        print(Fore.RED+"Rent Duration exceeds limit! Try again\n")
        time.sleep(1)
        userRentCar(carInfo)
    except:
      print(Fore.RED+"Rent Duration has to be an integer! Try again\n")
      time.sleep(1.5)
      userRentCar(carInfo)
    
    rentend = db.add_hours_to_utc(rentduration)
    date = datetime.fromtimestamp(rentend).strftime('%d %B %Y')
    dtime = datetime.fromtimestamp(rentend).strftime('%I:%M %p')
    print(Style.BRIGHT +Fore.CYAN +"\n> You will be charged "+Fore.GREEN+"₹"+str(carInfo[7])+Style.BRIGHT +Fore.CYAN+" every hour and your rent will end on "+Fore.GREEN+date+Style.BRIGHT +Fore.CYAN+" at "+Fore.GREEN+dtime)
    print(Style.BRIGHT +Fore.RED +"> If the vehicle is not returned on time, you will be penalised ₹850 for every day after the rent ends.")
    aa = input(Style.BRIGHT +Fore.GREEN +"> Confirm? (Y/N) : ")
    if aa.lower() == 'y':
      db.rentCarAdd(carInfo,uid,rentduration,int(time.time()),rentend)
      print(Fore.GREEN+"\n ==> Booking Success!\n")
      time.sleep(1)
      menu()
    else:
      print(Fore.RED+"Rent Cancelled!\n")
      time.sleep(1)
      userViewCars()
  else:
    print(Fore.RED+"Rent Cancelled!\n")
    time.sleep(1)
    userViewCars()

def userViewCars():
  global car_headers
  
  Banner()
  print(Style.BRIGHT +Fore.CYAN+"> Select Filter:\n")
  print(Style.BRIGHT +Fore.CYAN+"[1] View all Cars")
  print(Style.BRIGHT +Fore.CYAN+"[2] Filter by Brand")
  print(Style.BRIGHT +Fore.CYAN+"[3] Filter by Type")
  print(Style.BRIGHT +Fore.CYAN+"[4] Filter by Fuel")
  print(Style.BRIGHT +Fore.CYAN+"[5] Filter by Transmission")
  print(Style.BRIGHT +Fore.CYAN+"[6] Search a Car")
  print(Style.BRIGHT +Fore.CYAN+"[0] Go Back")
  selectedmodule = int(input(Style.BRIGHT +Fore.CYAN +"> "))
  
  if selectedmodule == 1:
    cars = db.getCars(userInfo[5])
    cardata = []
    for x in cars:
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],"₹"+str(x[7])])
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"[+] Total "+str(len(cardata))+" cars in "+userInfo[5]+":\n")
    print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Rent it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a.lower() == 'x':
      userViewCars()
    else:
      try:
        userRentCar(cars[int(a)])
      except Exception as e:
        userViewCars()
                 
  elif selectedmodule == 2:
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select Brand:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Audi")
    print(Style.BRIGHT +Fore.CYAN+"[2] BMW")
    print(Style.BRIGHT +Fore.CYAN+"[3] Chevrolet")
    print(Style.BRIGHT +Fore.CYAN+"[4] Ferrari")
    print(Style.BRIGHT +Fore.CYAN+"[5] Ford")
    print(Style.BRIGHT +Fore.CYAN+"[6] Koenigsegg")
    print(Style.BRIGHT +Fore.CYAN+"[7] Lamborghini")
    print(Style.BRIGHT +Fore.CYAN+"[8] Mahindra")
    print(Style.BRIGHT +Fore.CYAN+"[9] Mazda")
    print(Style.BRIGHT +Fore.CYAN+"[10] McLaren")
    print(Style.BRIGHT +Fore.CYAN+"[11] Nissan")
    print(Style.BRIGHT +Fore.CYAN+"[12] Porsche")
    print(Style.BRIGHT +Fore.CYAN+"[13] Toyota")
    print(Style.BRIGHT +Fore.CYAN+"[14] Volvo")
    selectedbrand = int(input(Style.BRIGHT +Fore.CYAN +"> "))

    cars = db.getCarsBrand(brand[selectedbrand])
    cardata = []
    for x in cars:
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],"₹"+str(x[7])])
      
    Banner()
    #print(Style.BRIGHT +Fore.CYAN+"[+] List of Available '"+brand[selectedbrand]+"' Cars:\n")
    print(Style.BRIGHT +Fore.CYAN+"[+] Total "+str(len(cardata))+" "+brand[selectedbrand]+" cars in "+userInfo[5]+":\n")
    print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Rent it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a == 'x':
      userViewCars()
    else:
      try:
        userRentCar(cars[int(a)])
      except:
        userViewCars()
                 
  elif selectedmodule == 3:
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select Type:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Sedan")
    print(Style.BRIGHT +Fore.CYAN+"[2] Hatchback")
    print(Style.BRIGHT +Fore.CYAN+"[3] SUV")
    print(Style.BRIGHT +Fore.CYAN+"[4] MUV")
    print(Style.BRIGHT +Fore.CYAN+"[5] Coupe")
    print(Style.BRIGHT +Fore.CYAN+"[6] Sports")
    selectedtype = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    cars = db.getCarsType(car_type[selectedtype])
    Banner()
    cardata = []
    for x in cars:
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],"₹"+str(x[7])])
      
    Banner()
    #print(Style.BRIGHT +Fore.CYAN+"[+] List of Available '"+car_type[selectedtype]+"' Cars:\n")
    print(Style.BRIGHT +Fore.CYAN+"[+] Total "+str(len(cardata))+" "+car_type[selectedtype]+" cars in "+userInfo[5]+":\n")
    print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Rent it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a == 'x':
      userViewCars()
    else:
      try:
        userRentCar(cars[int(a)])
      except:
        userViewCars()
         
  elif selectedmodule == 4:
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select Fuel:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Diesel")
    print(Style.BRIGHT +Fore.CYAN+"[2] Petrol")
    print(Style.BRIGHT +Fore.CYAN+"[3] Electric")
    selectedfuel = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    cars = db.getCarsFuel(car_fuel[selectedfuel])
    Banner()
    cardata = []
    for x in cars:
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],"₹"+str(x[7])])
      
    Banner()
    #print(Style.BRIGHT +Fore.CYAN+"[+] List of Available '"+car_fuel[selectedfuel]+"' Cars:\n")
    print(Style.BRIGHT +Fore.CYAN+"[+] Total "+str(len(cardata))+" "+car_fuel[selectedfuel]+" cars in "+userInfo[5]+":\n")
    print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Rent it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a == 'x':
      userViewCars()
    else:
      try:
        userRentCar(cars[int(a)])
      except:
        userViewCars()
         
  elif selectedmodule == 5:
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select Transmission:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Manual")
    print(Style.BRIGHT +Fore.CYAN+"[2] Automatic")
    selectedtrans= int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    cars = db.getCarsTM(car_transmission[selectedtrans])
    Banner()
    cardata = []
    for x in cars:
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],"₹"+str(x[7])])
      
    Banner()
    #print(Style.BRIGHT +Fore.CYAN+"[+] List of Available '"+car_transmission[selectedtrans]+"' Cars:\n")
    print(Style.BRIGHT +Fore.CYAN+"[+] Total "+str(len(cardata))+" "+car_transmission[selectedtrans]+" cars in "+userInfo[5]+":\n")
    print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Rent it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a == 'x':
      userViewCars()
    else:
      try:
        userRentCar(cars[int(a)])
      except:
        userViewCars()
  
  elif selectedmodule == 6 :
    
    Banner()
    selectedsearch = input(Style.BRIGHT +Fore.CYAN +"> Search for : ")
    cars = db.getCarsSearch(selectedsearch)
    Banner()

    if cars == False or cars == []:
      print(Style.BRIGHT +Fore.RED+"[-] No Matches found for '"+selectedsearch+"'")
    else: 
      cardata = []
      for x in cars:
        cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],"₹"+str(x[7])])
        
      Banner()
      print(Style.BRIGHT +Fore.CYAN+"[+] List of Available '"+selectedsearch+"' Cars:\n")
      print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Rent it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a == 'x':
      userViewCars()
    else:
      try:
        userRentCar(cars[int(a)])
      except:
        userViewCars()
  
  elif selectedmodule == 0:
    menu()
  
  else:
    print(Fore.RED+"Invalid Choice! Try Again\n")
    time.sleep(1)
    userViewCars()

def userMenu():
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select Option:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] View Cars")
    print(Style.BRIGHT +Fore.CYAN+"[2] Return a Car")
    print(Style.BRIGHT +Fore.CYAN+"[3] Your Rental History")
    print(Style.BRIGHT +Fore.CYAN+"[4] Your Account")
    print(Style.BRIGHT +Fore.CYAN+"[5] Terms and Conditions")
    print(Style.BRIGHT +Fore.CYAN+"[0] Logout")
    selectedmodule = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    executechecker(selectedmodule)
   
def userTerms():
  Banner()
  print(Style.BRIGHT +Fore.GREEN+"> Terms and Conditions:\n")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Rental Period:"+Style.BRIGHT +Fore.CYAN+" Users can rent a car for a maximum period of 240 hours (10 days) from the time of pick-up.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Penalty:"+Style.BRIGHT +Fore.CYAN+" A penalty of ₹850 will be charged for each day the car is delayed beyond the scheduled return date.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Pick-Up and Return:"+Style.BRIGHT +Fore.CYAN+" Users can pick up the rented car from the nearest RentRide point. \nSimilarly, the car must be returned to any of the designated RentRide points upon completion of the rental period.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Payment:"+Style.BRIGHT +Fore.CYAN+" Users have the option to pay the rental charges at the time of returning the car. \nThe rental charges will include the base rental fee for the duration of the rental and any penalty charges for delays.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Car Condition:"+Style.BRIGHT +Fore.CYAN+" Users are responsible for returning the car in the same condition as it was received, \nsubject to reasonable wear and tear. \nAny damages beyond normal wear and tear will be subject to additional repair charges.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Valid Identification:"+Style.BRIGHT +Fore.CYAN+" Users must present a valid government-issued identification and a valid driver's license \nat the time of picking up the car.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Violation of Terms:"+Style.BRIGHT +Fore.CYAN+" RentRide reserves the right to refuse rental service to users who violate \nany of the terms and conditions or display inappropriate behavior during the rental period.")
  
  a = input(Style.DIM+Fore.CYAN +"\n\n> Press Enter to go back")
  menu()


##=======[PARTNER FUNCTIONS]=========##
def partnerCars():
    cars = db.getPartnerCars(uid)
    cardata = []
    for x in cars:
      statusx = x[10]
      if statusx == 0:
        status = Fore.RED+"ON RENT"+Fore.CYAN+Style.BRIGHT
      else:
        status = Fore.GREEN+"AVAILABLE"+Fore.CYAN+Style.BRIGHT
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],x[7],status])
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"[+] List of Your Cars:\n")
    print(tabulate.tabulate(cardata, headers=partner_car_headers, tablefmt="rounded_grid", disable_numparse=True))
    a = input(Style.BRIGHT +Fore.GREEN +"\n> Press Enter to go back ")
    partnerMenu()

def partnerEditCars():
    cars = db.getPartnerCars(uid)
    cardata = []
    for x in cars:
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],x[7]])
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"[+] List of Your Cars:\n")
    print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Edit it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a == 'x':
      partnerMenu()
    else:
      Banner()
      carInfo = cars[int(a)]
      brand = carInfo[1]
      model = carInfo[2]
      ctype = carInfo[3]
      fuel = carInfo[4]
      seating = carInfo[5]
      trans = carInfo[6]
      rate = carInfo[7]
      location = carInfo[9]
      print(Style.BRIGHT +Fore.CYAN+"> Details:\n")
      print(Style.BRIGHT +Fore.CYAN+"[+] Brand : "+brand)
      print(Style.BRIGHT +Fore.CYAN+"[+] Model : "+model)
      print(Style.BRIGHT +Fore.CYAN+"[+] Type : "+ctype)
      print(Style.BRIGHT +Fore.CYAN+"[+] Fuel : "+fuel)
      print(Style.BRIGHT +Fore.CYAN+"[+] Seating Capacity : "+str(seating))
      print(Style.BRIGHT +Fore.CYAN+"[+] Transmission : "+trans)
      print(Style.BRIGHT +Fore.CYAN+"[+] Rate : ₹"+str(rate)+" per hour")
      print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+location)
      
      selectededit = input(Style.BRIGHT +Fore.GREEN +"\n> Do you want to edit this car? (Y/N) : ")
      if selectededit.lower() != 'y':
        partnerMenu()
      else:
        Banner()
        print(Style.BRIGHT +Fore.RED+"[-] Leave any field blank to use existing value:\n")
        
        cbrand = input(Style.BRIGHT +Fore.CYAN +"> Enter New Car Brand :")
        
        if cbrand == '' or cbrand.strip() == '':
          cbrand = brand
        cmodel = input(Style.BRIGHT +Fore.CYAN +"> Enter New Car Model :")
        
        if cmodel == '' or cmodel.strip() == '':
          cmodel = model
        
        print(Style.BRIGHT +Fore.CYAN+"\n> Select New Car Type:\n")
        print(Style.BRIGHT +Fore.CYAN+"[1] Sedan")
        print(Style.BRIGHT +Fore.CYAN+"[2] Hatchback")
        print(Style.BRIGHT +Fore.CYAN+"[3] SUV")
        print(Style.BRIGHT +Fore.CYAN+"[4] MUV")
        print(Style.BRIGHT +Fore.CYAN+"[5] Coupe")
        print(Style.BRIGHT +Fore.CYAN+"[6] Sports")
        cctype = input(Style.BRIGHT +Fore.CYAN +"> ")
        
        if cctype == '' or cctype.strip() == '':
          cctype = ctype
        else:
          ctype = car_type[int(cctype)]
        
        print(Style.BRIGHT +Fore.CYAN+"\n> Select New Car Fuel Type:\n")
        print(Style.BRIGHT +Fore.CYAN+"[1] Diesel")
        print(Style.BRIGHT +Fore.CYAN+"[2] Petrol")
        print(Style.BRIGHT +Fore.CYAN+"[3] Electric")
        ccfuel = input(Style.BRIGHT +Fore.CYAN +"> ")
        
        if ccfuel == '' or ccfuel.strip() == '':
          ccfuel = fuel
        else:
          ccfuel = car_fuel[int(ccfuel)]
        
        print(Style.BRIGHT +Fore.CYAN+"\n> Select New Car Transmission Type:\n")
        print(Style.BRIGHT +Fore.CYAN+"[1] Manual")
        print(Style.BRIGHT +Fore.CYAN+"[2] Automatic")
        cctrans = input(Style.BRIGHT +Fore.CYAN +"> ")
        
        if cctrans == '' or cctrans.strip() == '':
          cctrans = trans
        else:
          cctrans = car_transmission[int(cctrans)]
        
        crate = input(Style.BRIGHT +Fore.CYAN +"\n> Enter New Rate per hour :")
        
        if crate == '' or crate.strip() == '':
          crate = rate
        else:
          crate = float(crate)
          
        cseating = input(Style.BRIGHT +Fore.CYAN +"\n> Enter New Seating Capacity :")
        
        if cseating == '' or cseating.strip() == '':
          cseating = seating
        
        ccity = input(Style.BRIGHT +Fore.CYAN +"\n> Enter New City :")
        if ccity == '' or ccity.strip() == '':
          ccity = location
        
        Banner()
        print(Style.BRIGHT +Fore.CYAN+"> Edit Car:\n")
        print(Style.BRIGHT +Fore.CYAN+"[+] Brand : "+cbrand)
        print(Style.BRIGHT +Fore.CYAN+"[+] Model : "+cmodel)
        print(Style.BRIGHT +Fore.CYAN+"[+] Type : "+ctype)
        print(Style.BRIGHT +Fore.CYAN+"[+] Fuel : "+ccfuel)
        print(Style.BRIGHT +Fore.CYAN+"[+] Seating Capacity : "+cseating)
        print(Style.BRIGHT +Fore.CYAN+"[+] Transmission : "+cctrans)
        print(Style.BRIGHT +Fore.CYAN+"[+] Rate per hour : ₹"+str(crate))
        print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+ccity)
        
        a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to edit? (Y/N) : ')
        if a.lower() == 'y':
          db.editPartnerCar(carInfo[0],cbrand,cmodel,ctype,ccfuel,int(cseating),cctrans,crate,ccity)
          print(Fore.GREEN+"\n ==> Successfully Updated!\n")
          time.sleep(1)
          partnerMenu()
        else:
            print(Fore.RED+"Operation Cancelled!\n")
            time.sleep(1)
            partnerMenu()

def partnerRentalRecords():
  
  history = db.getPartnerRecords(uid)
  history_headers = ['ID','Brand','Model','Rented On','Duration','Price','Rented By','Status']
  hisdata = []
  for x in history:
    datebegin = datetime.fromtimestamp(int(x[3]))
    daydifference = ((datetime.now() - datebegin).total_seconds())/3600
    price=round(x[6]*daydifference)
    date = datetime.fromtimestamp(int(x[3])).strftime('%d %B %Y')
    if x[9] == '0':
      status = Fore.RED+'Not Returned'+Style.BRIGHT+Fore.CYAN
    elif x[9] == '1':
      status = Fore.GREEN+'Returned'+Style.BRIGHT+Fore.CYAN
    carInfo = db.getCarInfo(x[1])[0]
    hisdata.append([history.index(x),carInfo[1],carInfo[2],date,x[5]+" Hours","₹"+str(format(price, ',d')),db.getPartnerInfo(x[0])[0],status])
  Banner()
  
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Rental Records:\n")
  print(tabulate.tabulate(hisdata, headers=history_headers, tablefmt="rounded_grid", disable_numparse=True))
  
  a = input(Style.BRIGHT +Fore.CYAN +"\n> Press Enter to go back")
  menu()
  
def partnerAddCar():
    Banner()
    print(Style.BRIGHT +Fore.RED+"[-] Leave any field blank to go back:\n")
    
    cbrand = input(Style.BRIGHT +Fore.CYAN +"> Enter Car Brand :")
    
    if cbrand == '' or cbrand.strip() == '':
      partnerMenu()
    cmodel = input(Style.BRIGHT +Fore.CYAN +"> Enter Car Model :")
    
    if cmodel == '' or cmodel.strip() == '':
      partnerMenu()
    
    print(Style.BRIGHT +Fore.CYAN+"\n> Select Car Type:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Sedan")
    print(Style.BRIGHT +Fore.CYAN+"[2] Hatchback")
    print(Style.BRIGHT +Fore.CYAN+"[3] SUV")
    print(Style.BRIGHT +Fore.CYAN+"[4] MUV")
    print(Style.BRIGHT +Fore.CYAN+"[5] Coupe")
    print(Style.BRIGHT +Fore.CYAN+"[6] Sports")
    cctype = input(Style.BRIGHT +Fore.CYAN +"> ")
    
    if cctype == '' or cctype.strip() == '':
      partnerMenu()
    ctype = car_type[int(cctype)]
    
    print(Style.BRIGHT +Fore.CYAN+"\n> Select Car Fuel Type:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Diesel")
    print(Style.BRIGHT +Fore.CYAN+"[2] Petrol")
    print(Style.BRIGHT +Fore.CYAN+"[3] Electric")
    ccfuel = input(Style.BRIGHT +Fore.CYAN +"> ")
    
    if ccfuel == '' or ccfuel.strip() == '':
      partnerMenu()
    cfuel = car_fuel[int(ccfuel)]
    
    print(Style.BRIGHT +Fore.CYAN+"\n> Select Car Transmission Type:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Manual")
    print(Style.BRIGHT +Fore.CYAN+"[2] Automatic")
    cctrans = input(Style.BRIGHT +Fore.CYAN +"> ")
    
    if cctrans == '' or ccfuel.strip() == '':
      partnerMenu()
    ctrans = car_transmission[int(cctrans)]
    
    cseater = input(Style.BRIGHT +Fore.CYAN +"\n> Enter Seating Capacity :")
    
    crate = input(Style.BRIGHT +Fore.CYAN +"\n> Enter Rate per hour :")
    
    if crate == '' or crate.strip() == '':
      partnerMenu()
    else:
      crate = float(crate)
    
    ccity = input(Style.BRIGHT +Fore.CYAN +"\n> Enter City :")
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Add Car:\n")
    print(Style.BRIGHT +Fore.CYAN+"[+] Brand : "+cbrand)
    print(Style.BRIGHT +Fore.CYAN+"[+] Model : "+cmodel)
    print(Style.BRIGHT +Fore.CYAN+"[+] Type : "+ctype)
    print(Style.BRIGHT +Fore.CYAN+"[+] Fuel : "+cfuel)
    print(Style.BRIGHT +Fore.CYAN+"[+] Seating Capacity : "+cseater)
    print(Style.BRIGHT +Fore.CYAN+"[+] Transmission : "+ctrans)
    print(Style.BRIGHT +Fore.CYAN+"[+] Rate per hour : ₹"+str(crate))
    print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+ccity)
    
    a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to add? (Y/N) : ')
    if a.lower() == 'y':
      db.addPartnerCar(cbrand,cmodel,ctype,cfuel,cseater,ctrans,crate,ccity,uid)
      print(Fore.GREEN+"\n ==> Successfully Added!\n")
      time.sleep(1)
      partnerMenu()
    else:
        print(Fore.RED+"Operation Cancelled!\n")
        time.sleep(1)
        partnerMenu()
        
    #db.addPartnerCar(cbrand,cmodel,ctype,cfuel,ctrans,crate,ccity,uid)
      
def partnerDeleteCars():
    cars = db.getPartnerCars(uid)
    cardata = []
    for x in cars:
        cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],x[7]])
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"[+] List of Your Cars:\n")
    print(tabulate.tabulate(cardata, headers=partner_car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'\n[-] Enter ID of car to Delete it')
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter X to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    if a == 'x':
      partnerMenu()
    else:
      carInfo = cars[int(a)]
      Banner()
      print(Style.BRIGHT +Fore.CYAN+"> Delete Car:\n")
      print(Style.BRIGHT +Fore.CYAN+"[+] Brand : "+carInfo[1])
      print(Style.BRIGHT +Fore.CYAN+"[+] Model : "+carInfo[2])
      print(Style.BRIGHT +Fore.CYAN+"[+] Type : "+carInfo[3])
      print(Style.BRIGHT +Fore.CYAN+"[+] Fuel : "+carInfo[4])
      print(Style.BRIGHT +Fore.CYAN+"[+] Seating Capacity : "+str(carInfo[5]))
      print(Style.BRIGHT +Fore.CYAN+"[+] Transmission : "+carInfo[6])
      print(Style.BRIGHT +Fore.CYAN+"[+] Rate per hour : ₹"+str(carInfo[7]))
      print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+carInfo[9])
      
      a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to delete? (Y/N) : ')
      if a.lower() == 'y':
        db.deletePartnerCar(carInfo[0])
        print(Fore.GREEN+"\n ==> Successfully Deleted!\n")
        time.sleep(1)
        partnerMenu()
      else:
          print(Fore.RED+"Operation Cancelled!\n")
          time.sleep(1)
          partnerMenu()

def partnerYourAccount():
  global uid
  global userType
  global userInfo
  
  name = userInfo[4]
  uname = userInfo[2]
  location = userInfo[5]

  Banner()
  print(Style.BRIGHT +Fore.GREEN+"> Your Account:\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Name : "+name)
  print(Style.BRIGHT +Fore.CYAN+"[+] Username : "+uname)
  print(Style.BRIGHT +Fore.CYAN+"[+] Account Type : "+userType.title())
  print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+location)
  
  a = input(Style.DIM+Fore.CYAN +"\n> Press Enter to go back")
  partnerMenu()

def partnerStats():
  
  stats = db.getPartnerStats(uid)

  Banner()
  print(Style.BRIGHT +Fore.GREEN+"> Your Statistics:\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Total Cars : "+str(stats[0]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Rental Count : "+str(stats[1]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Cars currently on Rent : "+str(stats[2]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Total Income : ₹"+str(stats[3]))
  a = input(Style.DIM+Fore.CYAN +"\n> Press Enter to go back")
  partnerMenu()

def partnerInitMenu():
    global userType
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Continue as:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] User")
    print(Style.BRIGHT +Fore.CYAN+"[2] Partner")
    selectedmodule = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    if selectedmodule == 1:
      userType = 'user'
      userMenu()
    elif selectedmodule == 2:
      userType = 'partner'
      partnerMenu()
    else:
      print(Fore.RED+"Invalid Choice! Try Again\n")
      time.sleep(1)
      partnerInitMenu()
          
def partnerMenu():
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select Option:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Your Cars")
    print(Style.BRIGHT +Fore.CYAN+"[2] Add a Car")
    print(Style.BRIGHT +Fore.CYAN+"[3] Delete Car")
    print(Style.BRIGHT +Fore.CYAN+"[4] Edit Car Info")
    print(Style.BRIGHT +Fore.CYAN+"[5] Rental Records")
    print(Style.BRIGHT +Fore.CYAN+"[6] Statistics")
    print(Style.BRIGHT +Fore.CYAN+"[7] Your Account")
    print(Style.BRIGHT +Fore.CYAN+"[8] Terms and Condition")
    print(Style.BRIGHT +Fore.CYAN+"[0] Logout")
    selectedmodule = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    executechecker(selectedmodule)
 
def partnerTerms():
  Banner()
  print(Style.BRIGHT +Fore.GREEN+"> Terms and Conditions:\n")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Rental Listing:"+Style.BRIGHT +Fore.CYAN+" Partners can list their cars for rent on the platform. They have the flexibility \nto set the hourly rental price for their vehicles.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Earnings Split:"+Style.BRIGHT +Fore.CYAN+" Partners will receive 80% of the total rental amount paid by the renter for each booking. \nThe remaining 20% will be retained by the platform as a service fee.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Withdrawal:"+Style.BRIGHT +Fore.CYAN+" Partners can withdraw their earnings from the platform every 30 days. The platform will process \nthe payment through the preferred payment method specified by the partner.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Car Condition:"+Style.BRIGHT +Fore.CYAN+" Partners must ensure that their cars are in proper working condition and meet all safety and \nlegal requirements before listing them for rent.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Insurance:"+Style.BRIGHT +Fore.CYAN+"  Partners should have valid insurance coverage for their cars. The platform may require proof of \ninsurance before allowing the car to be listed.")
  print(Style.BRIGHT +Fore.CYAN+"\n[+] "+Fore.GREEN+"Prohibited Use:"+Style.BRIGHT +Fore.CYAN+" Partners should inform renters about any prohibited uses of their cars, such as smoking, \ntransporting pets, or off-road driving.")
  a = input(Style.DIM+Fore.CYAN +"\n\n> Press Enter to go back")
  menu()


##=======[ADMIN FUNCTIONS]=========##       
def adminInitMenu():
    global userType
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Continue as:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] User")
    print(Style.BRIGHT +Fore.CYAN+"[2] Admin")
    selectedmodule = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    if selectedmodule == 1:
      userType = 'user'
      userMenu()
    elif selectedmodule == 2:
      userType = 'admin'
      adminMenu()
    else:
      print(Fore.RED+"Invalid Choice! Try Again\n")
      time.sleep(1)
      partnerInitMenu()
      
def adminMenu():
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select Option:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] View All Users")
    print(Style.BRIGHT +Fore.CYAN+"[2] View Partners")
    print(Style.BRIGHT +Fore.CYAN+"[3] View All Cars")
    print(Style.BRIGHT +Fore.CYAN+"[4] Add a User")
    print(Style.BRIGHT +Fore.CYAN+"[5] Suspend a User")
    print(Style.BRIGHT +Fore.CYAN+"[6] Reactivate a User")
    print(Style.BRIGHT +Fore.CYAN+"[7] Delete a User")
    print(Style.BRIGHT +Fore.CYAN+"[8] Statistics")
    print(Style.BRIGHT +Fore.CYAN+"[0] Logout")
    selectedmodule = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    executechecker(selectedmodule)
 
def adminViewUsers():
    users = db.getUsers()
    userdata = []
    for x in users:
      if x[0][6] == 0:
        sus = Fore.GREEN+'Active'+Style.BRIGHT+Fore.CYAN
      else:
        sus = Fore.RED+'Suspended'+Style.BRIGHT+Fore.CYAN
      userdata.append([users.index(x),x[0][0],x[0][2],x[0][4],x[0][5],sus,x[1],x[2]])
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"[+] List of All Users:\n")
    print(tabulate.tabulate(userdata, headers=user_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter any key to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    adminMenu()
  
def adminViewPartners():
    users = db.getPartners()
    userdata = []
    for x in users:
      userdata.append([users.index(x),x[0][0],x[0][2],x[0][4],x[0][5],x[1],x[2],x[3]])
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"[+] List of All Users:\n")
    print(tabulate.tabulate(userdata, headers=partner_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter any key to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    adminMenu()
  
def adminViewCars():
    cars = db.getAllCars()
    cardata = []
    for x in cars:
      cardata.append([cars.index(x),x[1],x[2],x[3],x[4],x[6],x[5],db.getPartnerInfo(x[8])[0],"₹"+str(x[7])])
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"[+] List of Available Cars:\n")
    print(tabulate.tabulate(cardata, headers=car_headers, tablefmt="rounded_grid", disable_numparse=True))
    print(Style.BRIGHT +Fore.GREEN+'[-] Enter any key to go back')
    a = input(Style.BRIGHT +Fore.GREEN +"\n> ")
    adminMenu()
  
def adminAddUser():
    Banner()
    print(Style.BRIGHT +Fore.RED+"[-] Leave any field blank to go back:\n")
    
    username = input(Style.BRIGHT +Fore.CYAN +"> Enter Username :")
    ucheck = db.unameAvailability(username)
    if ucheck == False:
      print(Fore.RED+" ==> Username already taken\n")
      time.sleep(1)
      adminAddUser()
    
    if username == '' or username.strip() == '':
      adminMenu()
      
    passwd = input(Style.BRIGHT +Fore.CYAN +"> Enter Password :")
    
    if passwd == '' or passwd.strip() == '':
      adminMenu()
    
    name = input(Style.BRIGHT +Fore.CYAN +"> Enter Name :")
    
    if name == '' or name.strip() == '':
      adminMenu()
    
    print(Style.BRIGHT +Fore.CYAN+"\n> Select Location:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Coimbatore")
    print(Style.BRIGHT +Fore.CYAN+"[2] Chennai")
    print(Style.BRIGHT +Fore.CYAN+"[3] Trichy")
    cloc = input(Style.BRIGHT +Fore.CYAN +"> ")
    
    if cloc == '' or cloc.strip() == '':
      adminMenu()
    try:
      location = locations[int(cloc)]
    except:
      adminMenu()
      
    print(Style.BRIGHT +Fore.CYAN+"\n> Select Type:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] User")
    print(Style.BRIGHT +Fore.CYAN+"[2] Partner")
    print(Style.BRIGHT +Fore.CYAN+"[3] Admin")
    ctype = input(Style.BRIGHT +Fore.CYAN +"> ")
    
    if ctype == '' or ctype.strip() == '':
      adminMenu()
    try:
      utype = utypes[int(ctype)]
    except:
      adminMenu()
      
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Add User:\n")
    print(Style.BRIGHT +Fore.CYAN+"[+] Username : "+username)
    print(Style.BRIGHT +Fore.CYAN+"[+] Password : "+passwd)
    print(Style.BRIGHT +Fore.CYAN+"[+] Name : "+name)
    print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+location)
    print(Style.BRIGHT +Fore.CYAN+"[+] Type : "+utype)
    
    a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to add? (Y/N) : ')
    if a.lower() == 'y':
      db.addUser(username,passwd,name,location,utype)
      print(Fore.GREEN+"\n ==> Successfully Added!\n")
      time.sleep(1)
      adminMenu()
    else:
        print(Fore.RED+"Operation Cancelled!\n")
        time.sleep(1)
        adminMenu()
      
def adminSuspendUser():
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Suspend User :\n")
    uname = input(Style.BRIGHT + Fore.CYAN + 'Enter Username: ')
    
    if db.unameAvailability(uname):
      print(Fore.RED+"User does not exist!")
      time.sleep(1)
      adminSuspendUser()
    else:
      userinfo = db.getUserInfoUname(uname)[0]
      Banner()
      print(Style.BRIGHT +Fore.CYAN+"> Suspend User:\n")
      print(Style.BRIGHT +Fore.CYAN+"[+] UID : "+userinfo[0])
      print(Style.BRIGHT +Fore.CYAN+"[+] Username : "+userinfo[2])
      print(Style.BRIGHT +Fore.CYAN+"[+] Name : "+userinfo[4])
      print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+userinfo[5])
      
      a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to suspend? (Y/N) : ')
      if a.lower() == 'y':
        db.suspendUser(uname,1)
        print(Fore.GREEN+"\n ==> Successfully Suspended!\n")
        time.sleep(1)
        adminMenu()
      else:
          print(Fore.RED+"Operation Cancelled!\n")
          time.sleep(1)
          adminMenu()
           
def adminUnSuspendUser():
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Reactivate User :\n")
    uname = input(Style.BRIGHT + Fore.CYAN + 'Enter Username: ')
    
    if db.unameAvailability(uname):
      print(Fore.RED+"User does not exist!")
      time.sleep(1)
      adminUnSuspendUser()
    else:
      userinfo = db.getUserInfoUname(uname)[0]
      Banner()
      print(Style.BRIGHT +Fore.CYAN+"> Reactivate User:\n")
      print(Style.BRIGHT +Fore.CYAN+"[+] UID : "+userinfo[0])
      print(Style.BRIGHT +Fore.CYAN+"[+] Username : "+userinfo[2])
      print(Style.BRIGHT +Fore.CYAN+"[+] Name : "+userinfo[4])
      print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+userinfo[5])
      
      a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to reactivate? (Y/N) : ')
      if a.lower() == 'y':
        db.suspendUser(uname,0)
        print(Fore.GREEN+"\n ==> Successfully Activated!\n")
        time.sleep(1)
        adminMenu()
      else:
          print(Fore.RED+"Operation Cancelled!\n")
          time.sleep(1)
          adminMenu()
             
def adminDeleteUser():
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Delete User :\n")
    uname = input(Style.BRIGHT + Fore.CYAN + 'Enter Username: ')
    
    if db.unameAvailability(uname):
      print(Fore.RED+"User does not exist!")
      time.sleep(1)
      adminDeleteUser()
    else:
      userinfo = db.getUserInfoUname(uname)[0]
      Banner()
      print(Style.BRIGHT +Fore.CYAN+"> Delete User:\n")
      print(Style.BRIGHT +Fore.CYAN+"[+] UID : "+userinfo[0])
      print(Style.BRIGHT +Fore.CYAN+"[+] Username : "+userinfo[2])
      print(Style.BRIGHT +Fore.CYAN+"[+] Name : "+userinfo[4])
      print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+userinfo[5])
      
      a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed to delete? (Y/N) : ')
      if a.lower() == 'y':
        db.deleteUser(uname)
        print(Fore.GREEN+"\n ==> Successfully Deleted!\n")
        time.sleep(1)
        adminMenu()
      else:
          print(Fore.RED+"Operation Cancelled!\n")
          time.sleep(1)
          adminMenu()

def adminStats():
  stats = db.getAdminStats()
  
  Banner()
  print(Style.BRIGHT +Fore.GREEN+"> Statistics:")
  print(Style.BRIGHT +Fore.GREEN+"\n>>> Userbase <<<\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Users : "+str(stats[0][0]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Suspended Users : "+str(stats[0][1]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Partners : "+str(stats[0][2]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Admins : "+str(stats[0][3]))
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Total Accounts : "+str(stats[0][4]))
  print(Style.BRIGHT +Fore.GREEN+"\n>>> Cars <<<\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Sedans : "+str(stats[1][0][0]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Hatchback : "+str(stats[1][0][1]))
  print(Style.BRIGHT +Fore.CYAN+"[+] SUV : "+str(stats[1][0][2]))
  print(Style.BRIGHT +Fore.CYAN+"[+] MUV : "+str(stats[1][0][3]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Coupe : "+str(stats[1][0][4]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Sports : "+str(stats[1][0][5]))
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Diesel : "+str(stats[1][1][0]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Petrol : "+str(stats[1][1][1]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Electric : "+str(stats[1][1][2]))
  print(Style.BRIGHT +Fore.CYAN+"\n[+] Total Cars : "+str(stats[1][1][0]+stats[1][1][1]+stats[1][1][2]))
  print(Style.BRIGHT +Fore.GREEN+"\n>>> Transcations <<<\n")
  print(Style.BRIGHT +Fore.CYAN+"[+] Total Transactions : "+str(stats[2][0]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Total Amount : ₹"+str(stats[2][1]))
  print(Style.BRIGHT +Fore.CYAN+"[+] Total Profit : ₹"+str(stats[2][2]))
  a = input(Style.BRIGHT+Fore.RED +"\n\n> Press Enter to go back")
  adminMenu()
  
  
##=======[COMMON FUNCTIONS]=========##    
def clear():
  ### Clears the Terminal ###
  os.system('cls')
  
def Banner():
    ### Prints the Title ###
    
    clear() 
    # center(os.get_terminal_size().columns) Centers the string in the console
    print(Style.BRIGHT +Fore.CYAN+"    ____             __     ____  _     __   ".center(os.get_terminal_size().columns))
    print(Style.BRIGHT +Fore.CYAN+"   / __ \___  ____  / /_   / __ \(_)___/ /__ ".center(os.get_terminal_size().columns))
    print(Style.BRIGHT +Fore.CYAN+"  / /_/ / _ \/ __ \/ __/  / /_/ / / __  / _ \ ".center(os.get_terminal_size().columns))
    print(Style.BRIGHT +Fore.CYAN+" / _, _/  __/ / / / /_   / _, _/ / /_/ /  __/".center(os.get_terminal_size().columns))
    print(Style.BRIGHT +Fore.CYAN+"/_/ |_|\___/_/ /_/\__/  /_/ |_/_/\__,_/\___/ \n".center(os.get_terminal_size().columns))
    print(Style.BRIGHT +Fore.CYAN+"[+]——— Car Rental Platform ———[+]\n".center(os.get_terminal_size().columns))

def executechecker(selectedmodule):
  global uid
  global userType
  
  if userType == 'user':
    if selectedmodule == 1:
      userViewCars()
    
    elif selectedmodule == 2:
      userReturnCar()
      
    elif selectedmodule == 3:
      userRentalHistory()
       
    elif selectedmodule == 4:
      userYourAccount()
    
    elif selectedmodule == 5:
      userTerms()
      
    elif selectedmodule == 0:
      registerorlogin()
    else:
      print(Fore.RED+"Invalid Choice! Try Again\n")
      time.sleep(1)
      userMenu()
      
  elif userType == 'partner':
    if selectedmodule == 1:
      partnerCars()
    
    elif selectedmodule == 2:
      partnerAddCar()
      
    elif selectedmodule == 3:
      partnerDeleteCars()
    
    elif selectedmodule == 4:
      partnerEditCars()
      
    elif selectedmodule == 5:
      partnerRentalRecords()
      
    elif selectedmodule == 6:
      partnerStats()  
       
    elif selectedmodule == 7:
      partnerYourAccount()
    
    elif selectedmodule == 8:
      partnerTerms()
    
    elif selectedmodule == 0:
      registerorlogin()
    else:
      print(Fore.RED+"Invalid Choice! Try Again\n")
      time.sleep(1)
      partnerMenu()

  elif userType == 'admin':
    if selectedmodule == 1:
      adminViewUsers()
    
    elif selectedmodule == 2:
      adminViewPartners()
      
    elif selectedmodule == 3:
      adminViewCars()
    
    elif selectedmodule == 4:
      adminAddUser()
      
    elif selectedmodule == 5:
      adminSuspendUser()
      
    elif selectedmodule == 6:
      adminUnSuspendUser()  
       
    elif selectedmodule == 7:
      adminDeleteUser()
    
    elif selectedmodule == 8:
      adminStats()
    
    elif selectedmodule == 0:
      registerorlogin()
    else:
      print(Fore.RED+"Invalid Choice! Try Again\n")
      time.sleep(1)
      partnerMenu()

def register():
    location = None
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Register :\n")
    uname = input(Style.BRIGHT + Fore.CYAN + 'Enter Username: ')
    if db.unameAvailability(uname) == False:
      print(Fore.RED+"Username already taken!")
      time.sleep(1)
      register()
    name = input(Fore.CYAN+'Enter Name: ')
    
    print(Style.BRIGHT +Fore.CYAN+"\n> Select Location:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Coimbatore")
    print(Style.BRIGHT +Fore.CYAN+"[2] Chennai")
    print(Style.BRIGHT +Fore.CYAN+"[3] Trichy")
    cloc = input(Style.BRIGHT +Fore.CYAN +"> ")
    
    if cloc == '' or cloc.strip() == '':
      register()
    try:
      location = locations[int(cloc)]
    except:
      register()
    pwd = input(Style.BRIGHT + Fore.CYAN + 'Enter Password: ')
    
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Register User:\n")
    print(Style.BRIGHT +Fore.CYAN+"[+] Username : "+uname)
    print(Style.BRIGHT +Fore.CYAN+"[+] Password : "+pwd)
    print(Style.BRIGHT +Fore.CYAN+"[+] Name : "+name)
    print(Style.BRIGHT +Fore.CYAN+"[+] Location : "+location)
    
    a = input(Style.BRIGHT +Fore.GREEN+'\n[-] Proceed? (Y/N) : ')
    if a.lower() == 'y':
      db.addUser(uname,pwd,name,location)
      print(Fore.GREEN+"\n ==> Successfully Registered!\n")
      time.sleep(1)
      login()
    else:
        print(Fore.RED+"Operation Cancelled!\n")
        time.sleep(1)
        registerorlogin()
    
def registerorlogin():
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Select:\n")
    print(Style.BRIGHT +Fore.CYAN+"[1] Login")
    print(Style.BRIGHT +Fore.CYAN+"[2] Register")
    selectedmodule = int(input(Style.BRIGHT +Fore.CYAN +"> "))
    
    if selectedmodule == 1:
      login()
    else:
      register()
        
def menu():
  if userType == 'user':
    userMenu()
  elif userType == 'partner':
    partnerInitMenu()
  elif userType == 'admin':
    adminInitMenu()
              
def login():
    global uid
    global userType
    global userInfo
  
    Banner()
    print(Style.BRIGHT +Fore.CYAN+"> Login :\n")
    uname = input(Style.BRIGHT + Fore.CYAN + 'Enter Username: ')
    pwd = input(Style.BRIGHT + Fore.CYAN + 'Enter Password: ')
    log = db.login(uname,pwd)
    if log != False and log != []:
      uid = log[0][0]
      userInfo = log[0]
      userType = log[0][1]
      menu()
    else:
      print(Fore.RED+"Invalid Credentials! Try Again\n")
      time.sleep(1)
      login()
      
      
registerorlogin() 
db.conn.close()