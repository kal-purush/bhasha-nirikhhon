import re

print("Welcome!!!")
print("This is a simple calculator with four functions,")
print("which are addition, subtraction, multiplication and division.")
print('You can input "help" for the usage example, "exit" to exit.')

while True:
	cmd = input(":")
	if cmd == "exit":
		exit()
	if cmd == "help":
		print("This calculator is just with the function of binary operation")
		print("Here are the examples:")
		print("1+1")
		print("1-1")
		print("2*2")
		print("3/2")
		print("Be careful that multiplication and division will be forced to excute float-point operation")
	num = re.findall("\d+\.\d|\d+", cmd)
	operator = re.findall("\\+|\\-|\\*|\\/",cmd)
	if operator[0] == "+":
		result = add(num);
	if operator[0] == "-":
		result = sub(num);
	if operator[0] == "*":
		result = mul(num);
	if operator[0] == "/":
		result = div(num);
	print(result)
	