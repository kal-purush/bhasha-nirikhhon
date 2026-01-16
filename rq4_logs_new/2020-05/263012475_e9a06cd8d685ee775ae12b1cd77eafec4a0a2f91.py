import socket

sockfd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sockfd.bind(('0.0.0.0', 54153))
print("My echo server is listening on port 54153\nIt is a calculator service in which you will give two numbers as input and an operator and you will get the result of it\nYou can use the following operators: \n+, -, *, \, %, ^, =, <, >")
while True:
	payload1, client_addr = sockfd.recvfrom(1024)
	payload2, client_addr = sockfd.recvfrom(1024)
	payload3, client_addr = sockfd.recvfrom(1024)
	if payload3[0] == '+':
		payload1 = float(payload1) + float(payload2)
		payload1 =  str(payload1)
		sockfd.sendto("The sum is: "+payload1+"\n", client_addr)
	elif payload3[0] == '-':
		payload1 = float(payload1) - float(payload2)
		payload1 =  str(payload1)
		sockfd.sendto("The subtraction result is: "+payload1+"\n", client_addr)
	elif payload3[0] == '*':
		payload1 = float(payload1) * float(payload2)
		payload1 =  str(payload1)
		sockfd.sendto("The product is: "+payload1+"\n", client_addr)  
	elif payload3[0] == '/':
		if float(payload2)==0:
			while float(payload2)==0:
				sockfd.sendto("Division by zero is not possible enter another number\n", client_addr)
				payload2, client_addr = sockfd.recvfrom(1024)
		payload1 = float(payload1) / float(payload2)
		payload1 =  str(payload1)
		sockfd.sendto("The Division is: "+payload1+"\n", client_addr)
	elif payload3[0] == '%':
		payload1 = float(payload1) % float(payload2)
		payload1 =  str(payload1)
		sockfd.sendto("The modolus is: "+payload1+"\n", client_addr)
	elif payload3[0] == '=':
		if float(payload1) == float(payload2):
			payload1 = "Equal"
		else:
			payload1 = "Not Equal"
		sockfd.sendto("The enterened numbers are "+payload1+"\n", client_addr)
	elif payload3[0] == '<':
		if float(payload1) < float(payload2):
			payload1 = "True"
		else:
			payload1 = "False"
		sockfd.sendto(payload1+"\n", client_addr)
	elif payload3[0] == '>':
		if float(payload1) > float(payload2):
			payload1 = "True"
		else:
			payload1 = "False"
		sockfd.sendto(payload1+"\n", client_addr)
	else:
		sockfd.sendto("Invalid operator\n", client_addr)		