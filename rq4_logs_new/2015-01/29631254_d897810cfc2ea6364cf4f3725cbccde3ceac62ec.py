#fuckin currency converter
#this si peice of shit, just look at those variabel names
#Todo: make it not a peice of shit
import sys

def errormessage():
	print("Proper Usage: AMMOUNT FROM TO")
	print("Example: 34.67 USA FRANC")
	print("Kown Codes:")
	for key in conversions:
		print(key)
	exit()	

conversions = {"USD": 	1.0,
			   "FRANC": 1.2,
			   "RUP": 	2.0,
			   "AUS": 	0.3,
			   "FUK": 	0.5}

#number checker
arguments = sys.argv

try:
	float(arguments[1])
except:
	errormessage()


if len(arguments) != 4 or arguments[2] not in conversions or arguments[3] not in conversions:
	errormessage()

ammount = arguments[1]
froom = arguments[2]
to = arguments[3]
print(float(ammount) * (conversions[froom]/conversions[to]))