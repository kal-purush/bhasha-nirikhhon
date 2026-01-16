# Python3 code to demonstrate working of 
# Converting binary to string 
# Using BinarytoDecimal(binary)+chr() 


# Defining BinarytoDecimal() function 
def BinaryToDecimal(binary): 
	
	# Using int function to convert to 
	# string 
	string = int(binary, 2) 
	
	return string 

def ConvertBinary(payload):
	# split string withput space
	items = payload.split( )

	# check length string 
	if len(items) < 7 or len(items) > 7000:
		return "unable-length"
	
	byte = ''
	# convert string decimal into string byte
	for i in range(0, len(items), 7):

		# slicing the bin_data from index range [0, 6] 
		# and storing it in temp_data 
		text_item = items[i:i + 7]
		
		# loop string to get bite on last string point
		for p in text_item:
			# change string into int
			number = int(p)
			# get mod value to get bite 1 or 0
			value = number%2
			# character for given all bite value 
			byte = byte+str(value)

	return byte



def Stageganography(item):
	bin_data = ConvertBinary(item)

	# check error if have length less than 7 and more than 7000
	if bin_data == "unable-length":
		print(bin_data)
		return

	# preapre variabel for result 
	str_data = ' '

	
	for i in range(0, len(bin_data), 7): 
      
		# slicing the bin_data from index range [0, 6] 
		# and storing it in temp_data 
		temp_data = bin_data[i:i + 7] 
		
		# passing temp_data in BinarytoDecimal() fuction 
		# to get decimal value of corresponding temp_data 
		decimal_data = BinaryToDecimal(temp_data) 
		
		# Deccoding the decimal value returned by  
		# BinarytoDecimal() function, using chr()  
		# function which return the string corresponding  
		# character for given ASCII value, and store it  
		# in str_data 
		str_data = str_data + chr(decimal_data)  
	
	return str_data


items = Stageganography('51 61 10 0 0 15 17')

print("result is :",items)
	