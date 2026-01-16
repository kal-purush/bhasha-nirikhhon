# Author: Abdulsalma taofeek akintunde  2014/1/50685
from socket import *

serverName = '127.0.0.1'
serverPort = 22595



clientSocket = socket(AF_INET, SOCK_DGRAM) 

message = [] 


operator = input("""
        WELCOME to my Math Server!! 
        What Operation would you like perform - ?       
                [1] Addition
                [2] Subtraction
                [3] Multiplication 
                [4] Division
                [5] Modulus
                >>>_
                """)

message.append(str(operator)) 

operand1 = str(input("enter  the first operand: >>>_ ")) 
operand2 = str(input("enter the second oprand: >>>_ ")) 

message.append(operand1) 
message.append(operand2) 


message = " ".join(message) 


clientSocket.sendto(message, (serverName, serverPort))
result, serverAddress = clientSocket.recvfrom(4096) 
print ("the server returned " + result + " as the answer")
clientSocket.close()