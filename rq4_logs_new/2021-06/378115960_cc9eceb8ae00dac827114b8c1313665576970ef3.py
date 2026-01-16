import socket
import json
from _thread import *
import sys
import hashlib
import time
import os
from os import system, name
from os import listdir
from os.path import isfile, join,realpath



####################################################
mypath = realpath(__file__).replace('node.py','')
thisfile = "node.py"
####################################################
files = []
copies = []
h_copies = []
h_files = []
accessing_files = False
ping_succ = True
req_queue =[]
fromsucc_queue = []

successor = ['0','0']
predecessor = ['0','0']
successor_id = '0'
predecessor_id = '0'
counter  =0
send_count = 0

pre_conn = []
join_conn = []
s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #listening for connections all the time
c = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #connecting to successor
f = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #finger table jump connections

finger_table = {}

########## listening for connections
def listening_thread(cport):
	global s
	global c
	global counter
	global successor_id
	global predecessor_id

	second_node = False
	first_connect = True
	if(successor_id != str(my_id)):
		#we need to recieve successor information before going into listening
		print("waiting for succ info")
		data = 'a'
		while("found_succ" not in data):
			data = (c.recv(1024)).decode('utf-8')
			data = json.loads(data)
			print('received from succ info: ', data)

			if "found_succ" in data["type"]:
				break;
		# If this was second node. Has to be hardcoded
		if(data['type'] ==  'found_succ_onlyone'):
			successor_id = data['succ_id']
			successor[1] = data['succ_port']
			predecessor_id = data['succ_id']
			predecessor[1] = data['succ_port']
			second_node = True
			start_new_thread(ping_successor,(successor[1],))
			start_new_thread(distribute_files,(True,))


		#connect to this succ and tell them I love you
		elif(data['type'] == 'found_succ'):
			successor[1] = data["succ_port"]
			successor_id = data["succ_id"]
			c = hello_successor(int(data['succ_port']),data['joining_id'],data['joining_port'], False)
			start_new_thread(ping_successor,(successor[1],))

		else:
			print("wth is this: ", data)
			#now that successor has set me as predecessor their prev predecessor will contact me
		


	
	while(1):

		conn,address = s.accept()
		if(second_node):
			pre_conn.insert(0,conn)
		
		print("Connected to ",address)
		if(first_connect):
			start_new_thread(listen_fromsucc,(conn,))
			start_new_thread(update_fingertable,(True,))
			#start_new_thread(distribute_files,(True,))
			first_connect = False
		start_new_thread(listen_requests,(conn,))
		

############# listen from successor
def listen_fromsucc(conn):
	global c
	global predecessor_id
	global successor_id
	global pre_conn
	raw =''
	while(1):
		try:

			data = (c.recv(1024)).decode('utf-8')
			raw = data
			#print("succ: ",data)
			
			if not data:
				#print('successor lost')
				#use finger table to find new dude
				find_new_succ(conn, str(my_id), str(node_port))


			if(data == ''):
				continue;
			
			if "}\n{" in data:
				reqs = data.split('\n')
				for req in reqs:
					fromsucc_queue.append(req)
			else:
				fromsucc_queue.append(data)

			if len(fromsucc_queue) > 0:
				data = fromsucc_queue.pop(0)
			else:
				continue;
			if(data == ''):
				continue;
			raw = data

			#print("succ: ",data)
			data = json.loads(data)
			
			#print('received from succ: ', data)
			

			if(data["type"] == "ping_reply"):
				#print('got ping reply, pre is: ', data["pre_id"])
				if(data["pre_id"] != str(my_id)):
					#if my successor is not identifying me as their predecessor
					#print("wah rey naye loug dhoond lieyey hain?")
					#set the node prev node returned to current succ
					successor[1] = data["pre_port"]
					successor_id  =  data["pre_id"]
					c =hello_successor(int(successor[1]),str(my_id),str(node_port),True)



			
			elif(data["type"] == "found_succ_onlyone"):
				#this dude is the only dude set this as my successor
				

				successor[1] = data["succ_port"]
				successor_id = data["succ_id"]
				predecessor[1] = data["succ_port"]
				predecessor_id = data["succ_id"]
				#print('My successor is: ', successor_id)
				#print('My predecessor is: ', predecessor_id)
				pre_conn.insert(0,conn)

				start_new_thread(ping_successor,(successor[1],))
				start_new_thread(update_fingertable,(True,))
				start_new_thread(distribute_files,(True,))



			elif(data["type"] == "found_succ"): #came from succ or joining via
				
				if(data["joining_id"] == str(my_id)):
					#print("found my succ with id "+ data["succ_id"])
					if(data["succ_port"] != successor[1]):
						successor[1] = data["succ_port"]
						successor_id = data["succ_id"]
						#connect to succ with greeting
						c = hello_successor(int(successor[1]),data["joining_id"],data["joining_port"],True)
					else:
						successor_id = data["succ_id"]
						successor[1] = data["succ_port"]

						hello_data = {}
						hello_data["type"] = "you_are_succ"; hello_data["joining_id"] = data['joining_id']; hello_data["joining_port"] = data['joining_port']

						string_data  = json.dumps(hello_data)+'\n'
						#print('send line 124')
						c.sendall(string_data.encode())
				else:
					string_data = json.dumps(data)+'\n'
				
					if(int(data["joining_via"]) < int(my_id)or int(data["joining_via"]) == int(predecessor_id)):
						pre_conn[0].sendall(string_data.encode())
					elif (int(data["joining_via"]) > int(my_id)):
							c.sendall(string_data.encode())
					elif(int(data["joining_via"])== int(my_id)):
						if(data["joining_port"] == "2" or data["joining_port"] == "3"):
							#do not send to joiner but add to finger table
							add_to_fingertable(data)
						elif("looking_to_send" in data["joining_port"]):
							#extract id and send the file to the correct successor
							print("successor for:", data["joining_id"],  " is ", data["succ_id"])
							fileid = data["joining_port"].replace("looking_to_send",'')
							start_new_thread(send_file,(fileid, data["succ_port"],False,False))
						else:
							join_conn[0].sendall(string_data.encode())

			elif(data["type"] == "find_succ"):
				c = put_node(conn,data['joining_id'],data['joining_port'], data['joining_via'])

			#graceful leave case coming from my succ(will contain new succ info)
			elif(data["type"] == "leaving"):
				successor_id = data["succ_id"]
				successor[1] = data["succ_port"]
				c = hello_successor(int(data["succ_port"]),str(my_id),str(node_port),False)
				#just need to connect to new successor wihtout triggering distribute


			else:
				print()
				print("CASE DROPPED!  from succ: ",data)
				print()

		except Exception:
			import traceback
			print(traceback.format_exc())
			print("raw:", raw)
			continue
				
############ listen from predecessor/joining nodes
def listen_requests(conn):
	global c
	global predecessor_id
	global successor_id
	raw =''
	while(1):
		try:
			data = (conn.recv(1024)).decode('utf-8')
			raw = data
			#print("req: ",data)

			if not data:
				print("LOST PREDECESSOR!")
				break;
			if(data == ''):
				continue;

			if "}\n{" in data:
				reqs = data.split('\n')
				for req in reqs:
					req_queue.append(req)
			else:
				req_queue.append(data)
			
			if(len(req_queue) > 0):
				data = req_queue.pop(0)
			else:
				continue;
			raw = data
			data = json.loads(data)
			
			#print("recieved from pre: ",data)
			joining_id = 0
			
			if(data["type"] == "init"):
				#this node is joining network/finding its place
				joining_id = data["joining_id"]
				joining_port = data["joining_port"]
				joining_via = data["joining_via_id"]
				#print("ID " + joining_id + " joined!")
				#start communicating to find successor
				join_conn.insert(0,conn)
				c = put_node(conn,joining_id,joining_port,joining_via)

			elif(data["type"] == "found_succ_onlyone"):
				#this dude is the only dude set this as my successor
				global successor_id

				successor[1] = data["succ_port"]
				successor_id = data["succ_id"]
				predecessor[1] = data["succ_port"]
				predecessor_id = data["succ_id"]
				#print('My successor is: ', successor_id)
				#print('My predecessor is: ', predecessor_id)
				pre_conn.insert(0,conn)
				start_new_thread(ping_successor,(successor[1],))
				start_new_thread(update_fingertable,(True,))
				start_new_thread(distribute_files,(True,))



			elif(data["type"] == "sending_file"):
				#prepare to write file
				fs = open(data["name"], 'w+b')
				
				if "_down" in data["name"]:
					
					done = False
					dat = conn.recv(1024)
					while dat:
						fs.write(dat)
						dat = conn.recv(1024)
						try:
							doneflag = dat.decode()
							if doneflag  == "done_sending":
								done = True
						except:
							pass;
					if not done:
						print("DOWNLOAD INTERUPTED")
						print("Contacting new peer!")
						actual_name = ''
						actual_name = data["name"].replace("_down",'')
						if "_copy" in actual_name:
							actual_name  = actual_name.replace("_copy",'')
						time.sleep(3)
						print("re downloading ", actual_name)
						start_new_thread(request_download,(str(actual_name),))

				else:
					dat = conn.recv(1024)
					while dat:
						fs.write(dat)
						dat = conn.recv(1024)
				
				

			#succ was found and the joining id is my id then I have found my successor yay!
			elif(data["type"] == "found_succ"): #came from pre
				#Since new node will be connected to a network node it will never have a predecessor so we just find joining via in this case
				#print('line 189 found succ')
				string_data = json.dumps(data) + '\n'
				
				if(int(data["joining_via"]) < int(my_id)or int(data["joining_via"]) == int(predecessor_id)):
					pre_conn[0].sendall(string_data.encode())
				elif (int(data["joining_via"]) > int(my_id)):
						c.sendall(string_data.encode())
				elif(int(data["joining_via"])== int(my_id)):
					if(data["joining_port"] == "2" or data["joining_port"] == "3"):
						#do not send to joiner but add to finger table
						add_to_fingertable(data)
					elif("looking_to_send"in data["joining_port"]):
						#extract id and send the file to the correct successor
						fileid = data["joining_port"].replace("looking_to_send",'')
						print("successor for:", data["joining_id"],  " is ", data["succ_id"])

						start_new_thread(send_file,(fileid, data["succ_port"],False,False))
					else:
						join_conn[0].sendall(string_data.encode())


			elif(data["type"] == "find_succ"):
				c = put_node(conn,data['joining_id'],data['joining_port'], data['joining_via'])


			elif(data["type"] == "you_are_succ"):
				#set predecessor
				predecessor[1] = data["joining_port"]
				predecessor_id = data["joining_id"]
				#print('New pre: ', predecessor_id)
				pre_conn.insert(0,conn)
				#since I am getting a new successor it is possible that I lost my predecessor
				if "2" in list(finger_table.keys()):
					del finger_table["2"]
				if "3" in list(finger_table.keys()):
					del finger_table["3"]
				
				start_new_thread(update_fingertable,(False,))
				if "leaving" not in list(data.keys()):
					start_new_thread(distribute_files,(False,))


			elif(data["type"] == "who_is_pre"):
				reply = {}
				reply["type"] = "ping_reply"
				reply["pre_id"] = str(predecessor_id)
				reply["pre_port"] = str(predecessor[1])
				string_data = json.dumps(reply)+'\n'
				conn.sendall(string_data.encode())

			elif(data["type"] == "file_not_found"):

				if int(data["joining_via"][0]) == int(my_id):
					print("File was not found!")

				else:

					filename = data["joining_port"].replace("need:",'')
					if filename in files or filename in copies:
						print("I have this file!")

						start_new_thread(send_file,(filename,data["joining_via"][1],False,True))
					else:
						#I dont have this file but I was rightful successor this must be in copies above me
						data_dic["type"] =  "file_not_found"
						data_dic["joining_id"] = data["joining_id"]
						data_dic["joining_port"] = data["joining_port"]
						data_dic["joining_via"] = data["joining_via"]

						string_data = json.dumps(data_dic)

						c.send(string_data.encode())

			elif(data["type"] == "leaving"):
				predecessor_id = data["pre_id"]
				predecessor_id[1] = data["pre_port"]
				start_new_thread(copy_to_succ,(False,))

			else:
				print()
				print("CASE DROPPED! handle : ",data)
				print()

		except Exception:
			continue
			import traceback
			print(traceback.format_exc())
			print("raw: ", raw)
			
			


################ ask for predecessor
def ping_successor(port):
	global c
	global predecessor_id
	global successor_id
	global ping_succ
	
	while(1):
		time.sleep(2)
		if not ping_succ:
			break;
		ping_data ={}
		ping_data["type"] = "who_is_pre";
		ping_data["from"] = str(my_id)
		string_data = json.dumps(ping_data)+'\n'
		#print('pinging ',successor_id, " ", successor[1])
		c.sendall(string_data.encode())

#################finger table maintenance
def add_to_fingertable(data):
	#print(data["succ_id"], " at pos " ,data["joining_port"])
	if(data['succ_id'] != str(my_id) and [data['succ_id'],data['succ_port']] not in list(finger_table.values())):
		finger_table[data['joining_port']] = [data['succ_id'], data['succ_port']]


def update_fingertable(bogus):
	global c
	global predecessor_id
	global successor_id

	
	while(1):
		time.sleep(3)
		finger_table["1"] = [successor_id, successor[1]]

		s_counter = 0
		for key in list(finger_table.keys()):
			if s_counter <=3  and successor_id != str(my_id):  #ONLY IF I AM NOT MY OWN SUCC
				try:
					s_counter = s_counter +1
					#ask for successor
					data_dic = {}
					data_dic["type"] = "find_succ"
					data_dic["joining_id"] = finger_table[key][0]
					data_dic["joining_port"] = "update_finger"
					data_dic["joining_via"] =  str(my_id)
					data_dic["hops"] = 10

					string_data = json.dumps(data_dic) +'\n'
					#print("succ for: ", finger_table[key][0])

					c.send(string_data.encode())
					time.sleep(0.2)
				except Exception:
					import traceback
					print(traceback.format_exc())
					continue;
			else:
				break
		
		if bogus == False:
			break;

########## correct successor after losing it
def find_new_succ(conn, joining_id, joining_port):
	#check if second entry exists: if it doesnt you are isolated
	global c
	global successor_id
	global predecessor_id

	if "2" not in list(finger_table.keys()):
		#I am isolated
		
		successor_id = str(my_id)
		successor[1] = str(node_port)
		predecessor_id = str(my_id)
		predecessor[1] = str(node_port)

		finger_table["1"] =[successor_id,successor[1]]
	else:
		successor_id = finger_table["2"][0]
		successor[1] = finger_table["2"][1]
		finger_table["1"] =[successor_id,successor[1]]
		del finger_table["2"]
		c = hello_successor(int(successor[1]),str(my_id),str(node_port),True)



########## connect to successor and inform it
def hello_successor(port,joining_id,joining_port,distribute):
	global c
	global successor_id
	global predecessor_id
	global ping_succ

	
	new_c = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #connecting to successor
	new_c.connect(('127.0.0.1', port))


	hello_data = {}
	hello_data["type"] = "you_are_succ"; hello_data["joining_id"] = joining_id; hello_data["joining_port"] = joining_port

	if not ping_succ:
		#this is a graceful leaving case add an additional field to indicate to succ
		hello_data["leaving"] = "yes"

	string_data  = json.dumps(hello_data)+'\n'
	print('contacting new successor')
	new_c.sendall(string_data.encode())
	if distribute:
		start_new_thread(distribute_files,(True,))


	return new_c


########## putting at right place
def put_node(conn,joining_id,joining_port,joining_via):
	global c
	global s
	global predecessor_id
	global successor_id
	global pre_conn
	global copies
	global files
	
	#if I am my successor i.e no other node is present then no need to replay I need to set this myself
	if(successor[1] == str(node_port) and (joining_port != "2" or joining_port != "3") and "looking_to_send" not in joining_port and joining_port != "update_finger" and "need:" not in joining_port):
			#tell this node I am your successor
			#print("I am the only node this is my successor")
			data_dic = {}
			data_dic["type"] = "found_succ_onlyone"; data_dic['succ_port'] = node_port; data_dic["joining_id"] = joining_id; 
			data_dic["joining_port"] = joining_port
			data_dic["succ_id"] = str(my_id)
			
			global successor_id
			successor[1] = joining_port
			successor_id = joining_id
			predecessor[1] =  joining_port
			predecessor_id = joining_id
			pre_conn.insert(0, conn)

			#print('My successor is: ', successor_id)
			#print('My predecessor is: ', predecessor_id)


			string_data = json.dumps(data_dic)+ '\n'
			conn.sendall(string_data.encode())
			

			c.connect(('127.0.0.1',int(joining_port)))
			

			start_new_thread(ping_successor,(successor[1],))
			start_new_thread(distribute_files,(True,))

			


		
	else: #THERE ARE ATLEAST TWO NODES PRESENT	
		#if node is bigger
		if(int(joining_id) == int(my_id) and joining_port == "update_finger"):
			#this is an update fingertable request I NEED TO SEND MY SUCCESSOR NOT MYSELF
			data_dic = {}
			data_dic["type"] = "found_succ"; data_dic['succ_port'] = successor[1]; data_dic["joining_id"] = joining_id; 
			

			if(int(joining_via) == int(predecessor_id)):
				data_dic["joining_port"] = "2"
			else:
				data_dic["joining_port"] = "3"
			
			data_dic["succ_id"] = successor_id
			data_dic["joining_via"] =  joining_via

			string_data = json.dumps(data_dic)+ '\n'
			#print("answer: to "+ joining_via + " " + string_data)

			if(int(data_dic["joining_via"]) < int(my_id) or int(data_dic["joining_via"]) == int(predecessor_id)):
					
					conn.sendall(string_data.encode())
			elif(int(data_dic["joining_via"]) > int(my_id) or int(data_dic["joining_via"]) == int(successor_id)):
					c.sendall(string_data.encode())
			elif(int(data_dic["joining_via"])== int(my_id)):
				if(data_dic["joining_port"] == "2" or data_dic["joining_port"] == "3"):
					#do not send to joiner but add to finger table
					add_to_fingertable(data)
				elif("looking_to_send" in data_dic["joining_port"]):
					#extract id and send the file to the correct successor
					fileid = data_dic["joining_port"].replace("looking_to_send",'')
					print("successor for:", data_dic["joining_id"],  " is ", data_dic["succ_id"])

					start_new_thread(send_file,(fileid, data_dic["succ_port"],False,False))
				else:
					join_conn[0].sendall(string_data.encode())
			

		
		elif(int(joining_id) == int(my_id) and "looking_to_send" in joining_port): ## exact successor which should hold the file
			
			data_dic = {}
			data_dic["type"] = "found_succ"; data_dic['succ_port'] = node_port; data_dic["joining_id"] = joining_id; 
			
			
			data_dic["joining_port"] = 	joining_port
			
			data_dic["succ_id"] = str(my_id)
			data_dic["joining_via"] =  joining_via

			string_data = json.dumps(data_dic)+ '\n'


			if(int(data_dic["joining_via"]) < int(my_id) or int(data_dic["joining_via"]) == int(predecessor_id)):
					pre_conn[0].sendall(string_data.encode())
			elif(int(data_dic["joining_via"]) > int(my_id)):
					c.sendall(string_data.encode())
			elif(int(data_dic["joining_via"])== int(my_id)):
				if(data_dic["joining_port"] == "2" or data_dic["joining_port"] == "3"):
					#do not send to joiner but add to finger table
					add_to_fingertable(data)
				elif("looking_to_send" in data_dic["joining_port"]):
					#extract id and send the file to the correct successor
					fileid = data_dic["joining_port"].replace("looking_to_send",'')
					print("successor for:", data_dic["joining_id"],  " is ", data_dic["succ_id"])

					start_new_thread(send_file,(fileid, data_dic["succ_port"],False,False))
				else:
					join_conn[0].sendall(string_data.encode())

			


		elif(int(joining_id) < int(my_id) and int(joining_id) > int(predecessor_id)):
			#print("I am the successor sending info back")
			 
			data_dic = {}
			
			if "need:" in joining_port:
				filename = joining_port.replace("need:",'')
				if filename in files or filename in copies:
					print("I have this file!")
					start_new_thread(send_file,(filename,joining_via[1],False,True))
				else:
					#I dont have this file but I was rightful successor this must be in copies above me
					data_dic["type"] =  "file_not_found"
					data_dic["joining_id"] = joining_id
					data_dic["joining_port"] = joining_port
					data_dic["joining_via"] = joining_via

					string_data = json.dumps(data_dic)

					c.send(string_data.encode())

			else:

				data_dic["type"] = "found_succ"; data_dic['succ_port'] = node_port; data_dic["joining_id"] = joining_id;

				data_dic["joining_port"] = joining_port
				
				data_dic["succ_id"] = str(my_id)
				data_dic["joining_via"] =  joining_via

				string_data = json.dumps(data_dic)+ '\n'

				if(int(data_dic["joining_via"]) < int(my_id)or int(data_dic["joining_via"]) == int(predecessor_id)):
						pre_conn[0].sendall(string_data.encode())
				elif(int(data_dic["joining_via"]) > int(my_id)):
						c.sendall(string_data.encode())
				elif(int(data_dic["joining_via"])== int(my_id)):
					if(data_dic["joining_port"] == "2" or data_dic["joining_port"] == "3"):
						#do not send to joiner but add to finger table
						add_to_fingertable(data)
					elif("looking_to_send" in data_dic["joining_port"]):
						#extract id and send the file to the correct successor
						fileid = data_dic["joining_port"].replace("looking_to_send",'')
						print("successor for:", data_dic["joining_id"],  " is ", data_dic["succ_id"])

						start_new_thread(send_file,(fileid, data_dic["succ_port"],False,False))
					else:
						join_conn[0].sendall(string_data.encode())

			


		elif (int(joining_id) > int(my_id) and int(successor_id) > int(my_id)):
			#print ("ID " + joining_id + " joined. Dude is bigger than me relaying to successor")
			data_dic = {}
			data_dic["type"] = "find_succ"
			data_dic["joining_id"] = joining_id
			data_dic["joining_port"] = joining_port
			data_dic["joining_via"] =  joining_via

			string_data = json.dumps(data_dic)+ '\n'
			c.sendall(string_data.encode())

			


		elif (int(joining_id) > int(my_id) and int(successor_id) < int(my_id)):
			#new max entry my succ is the succ
			#print ("ID " + joining_id + " joined. New MAX entry telling joining via")
			
			data_dic = {}

			if "need:" in joining_port:
				
				#I dont have this file but I was rightful successor this must be in copies above me
				data_dic["type"] =  "file_not_found"
				data_dic["joining_id"] = joining_id
				data_dic["joining_port"] = joining_port
				data_dic["joining_via"] = joining_via

				string_data = json.dumps(data_dic)

				c.send(string_data.encode())

			else:


				data_dic["type"] = "found_succ"; data_dic['succ_port'] = successor[1]; data_dic["joining_id"] = joining_id; 
				
				
				data_dic["joining_port"] = joining_port
				
				data_dic["succ_id"] = successor_id
				data_dic["joining_via"] =  joining_via
				
				string_data = json.dumps(data_dic)+ '\n'


				if(int(data_dic["joining_via"]) < int(my_id)):
						pre_conn[0].sendall(string_data.encode())
				elif(int(data_dic["joining_via"]) > int(my_id)):
						c.sendall(string_data.encode())
				elif(int(data_dic["joining_via"])== int(my_id)):
					if(data_dic["joining_port"] == "2" or data_dic["joining_port"] == "3"):
						#do not send to joiner but add to finger table
						add_to_fingertable(data)
					elif("looking_to_send" in data_dic["joining_port"]):
						#extract id and send the file to the correct successor
						fileid = data_dic["joining_port"].replace("looking_to_send",'')
						print("successor for:", data_dic["joining_id"],  " is ", data_dic["succ_id"])

						start_new_thread(send_file,(fileid, data_dic["succ_port"],False,False))
						
					else:
						join_conn[0].sendall(string_data.encode())

			



		elif (int(joining_id) < int(my_id) and int(predecessor_id) < int(my_id)):
			#print ("ID " + joining_id + " joined. Dude is smoller than me relaying to predecessor")
			data_dic = {}
			data_dic["type"] = "find_succ"
			data_dic["joining_id"] = joining_id
			data_dic["joining_port"] = joining_port
			data_dic["joining_via"] =  joining_via

			string_data = json.dumps(data_dic)+ '\n'
			pre_conn[0].sendall(string_data.encode())

			


		elif (int(joining_id) < int(my_id) and int(predecessor_id) > int(my_id)):
			#new min entry my pre is the succ
			#print ("ID " + joining_id + " joined. New Min I am the succ, telling joining via")

			data_dic = {}

			if "need:" in joining_port:
				filename = joining_port.replace("need:",'')
				if filename in files or filename in copies:
					print("I have this file!")

					start_new_thread(send_file,(filename,joining_via[1],False,True))
				else:
					#I dont have this file but I was rightful successor this must be in copies above me
					data_dic["type"] =  "file_not_found"
					data_dic["joining_id"] = joining_id
					data_dic["joining_port"] = joining_port
					data_dic["joining_via"] = joining_via

					string_data = json.dumps(data_dic)

					c.send(string_data.encode())
			else:		

				data_dic["type"] = "found_succ"; data_dic['succ_port'] = str(node_port); data_dic["joining_id"] = joining_id; 
				
			
				data_dic["joining_port"] = joining_port
				
				data_dic["succ_id"] = str(my_id)
				data_dic["joining_via"] =  joining_via

				string_data = json.dumps(data_dic)+ '\n'

				if(int(data_dic["joining_via"]) < int(my_id)):
						pre_conn[0].sendall(string_data.encode())
				elif(int(data_dic["joining_via"]) > int(my_id)):
						c.sendall(string_data.encode())
				elif(int(data_dic["joining_via"])== int(my_id)):
					if(data_dic["joining_port"] == "2" or data_dic["joining_port"] == "3"):
						#do not send to joiner but add to finger table
						add_to_fingertable(data)
					elif("looking_to_send" in data_dic["joining_port"]):
						#extract id and send the file to the correct successor
						fileid = data_dic["joining_port"].replace("looking_to_send",'')
						print("successor for:", data_dic["joining_id"],  " is ", data_dic["succ_id"])
						
						start_new_thread(send_file,(fileid, data_dic["succ_port"],False,False))
					else:
						join_conn[0].sendall(string_data.encode())

		else:
			print()
			print("CASE DROPPED!  : joining id: ", joining_id, " joining_port: ", joining_port, " joining_via: ", joining_via)
			print()
	
	return c


################# distribute files like joining nodes
def distribute_files(useless): #send requests for successors
	if successor_id != str(my_id):
		time.sleep(1.2)
		if(not useless):
			time.sleep(0.5)
		global c
		global f
		global accessing_files
		global h_files
		global thisfile
		global mypath

		file_log(True)

		accessing_files = True

		for fileno in range(0, len(files)):
			#search finger table first
			max_id = 0
			max_port = 0
			
			send_to_succ  = False
			try:
				if([str(my_id), str(node_port)] not in list(finger_table.values())  and  len(list(finger_table.keys())) > 1):
					for value in list(finger_table.values()):
						if(int(value[0]) > int(max_id) and int(h_files[fileno])> int(value[0])):
							max_id = int(value[0])
							max_port = int(value[1])
				else: #send to succesor
					send_to_succ = True


				#ask for successor for this file from this node:
				data_dic = {}
				data_dic["type"] = "find_succ"
				data_dic["joining_id"] = str(h_files[fileno])
				data_dic["joining_port"] = "looking_to_send"+str(fileno)
				data_dic["joining_via"] =  str(my_id)

				string_data = json.dumps(data_dic)+ '\n'

				if(not send_to_succ and int(max_port) != 0):
					print("asking for succ: ",files[fileno], " id: ", h_files[fileno], " to ",max_id," ",max_port )
					fsock = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #finger table jump connections
					fsock.connect(('127.0.0.1',int(max_port)))
					fsock.sendall(string_data.encode())
					

					f = fsock
		
				else:
					print("asking for succ: ",files[fileno], " id: ", h_files[fileno], " to successor")
					c.sendall(string_data.encode())
					

				time.sleep(0.1)
			except Exception:
				import traceback
				print(traceback.format_exc())
				print("SOME PROBLEM WITH DISTRIBUTE")
				continue;
		accessing_files = False
		
		time.sleep(4.5)
		
		file_log(True)
		
		copy_to_succ(True)
		
#################################
def graceful_leave():

	global files
	global ping_succ
	global pre_conn
	#kill ping
	ping_succ = False
	#send all files to successor
	for i in range(0, len(files)):
		start_new_thread(send_file,(i,successor[1],False,False))
		time.sleep(0.01)
	
	#make msg for succ
	msg_succ = {}
	msg_succ["type"] = "leaving"
	msg_succ["pre_id"] = predecessor_id
	msg_succ["pre_port"] = predecessor[1]

	#make msg for pre
	msg_pre = {}
	msg_pre["type"] = "leaving"
	msg_pre["succ_id"] = successor_id
	msg_pre["succ_port"] = successor[1]

	#wait for ping to die
	time.sleep(3)

	#dispatch first to successor so it sets its pre for ping replies
	string_data = json.dumps(msg_succ)+'\n'
	c.sendall(string_data.encode())

	#wait
	time.sleep(0.2)
	
	#dispatch to predecessor
	string_data = json.dumps(msg_pre)+'\n'
	pre_conn[0].sendall(string_data.encode())

	time.sleep(0.2)
	

		
#################################
def copy_to_succ(useless):
	if not useless:
		time.sleep(4.5)
	
	if(int(successor_id) != int(my_id)):
			print("initiating copy!")

			for x in range(0, len(files)):
				# all the files in the array:
				try:
					#send to succ
					start_new_thread(send_file,(x,int(successor[1]),True,False))
					time.sleep(0.01)
				
				except Exception:
					import traceback
					print(traceback.format_exc())
					print("SOME PROBLEM WITH COPY")
					continue;
	time.sleep(4)
	file_log(False)

#######################################
def request_download(filename):

	if int(successor_id) == int(my_id):
		print("You must be connected to at least one node!")
		return;

	#can request a download
	hashedid = get_hash(str(filename))

	down_req = {}
	down_req["type"] = "find_succ"
	down_req["joining_id"] = str(hashedid)
	down_req["joining_port"] = "need:"+str(filename)
	down_req["joining_via"] = [str(my_id), str(node_port)]

	string_data = json.dumps(down_req) +'\n'


	c.send(string_data.encode())


	



#####################################
def send_file(fileid,port,copy,down):
	global node_port
	global f
	global accessing_files
	global send_count
	send_count = send_count +1
	if(int(port) != int(node_port)):
		accessing_files = True
		try:
			fromcopy = False

			if down:
				if fileid in files:
					indexno = files.index(fileid)
				elif fileid in copies:
					indexno = copies.index(fileid)
					fromcopy = True
				fileid = str(indexno)

			filename =''
			if not fromcopy:
				filename = files[int(fileid)]
			else:
				filename = copies[int(fileid)] +"_copy"

			print("sending: "+str(fileid))
			fs = open(filename, 'rb')

			fsock = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
			fsock.connect(('127.0.0.1',int(port)))
			dat_dic = {}
			dat_dic["type"] = "sending_file"
			sendname = filename
			
			if copy:
				sendname = filename +"_copy"
			elif down:
				sendname = filename + "_down"
			
			dat_dic["name"] = sendname


			string_data = json.dumps(dat_dic)+'\n'
			fsock.sendall(string_data.encode())
			time.sleep(0.1)

			dat = fs.read(1024)

			while (dat):
				#send bytes:
				
				fsock.sendall(dat)
				dat = fs.read(1024)
				time.sleep(0.01)
			f = fsock
			if not copy and not down:
				os.remove(filename)

			if down:
				done = "done_sending"
				fsock.sendall(done.encode())

		except Exception:
			import traceback
			print(traceback.format_exc())
			pass
		accessing_files = False
		send_count = send_count -1

########## SHA1 hash
def get_hash(string):
	hashobj = hashlib.sha1(string.encode()) 
	hashval = abs(int(hashobj.hexdigest(),16)) %1000
	return hashval



########## MAIN #####################

print('Enter YOUR port')
node_ip =  '127.0.0.1'
node_port = int(input())

############### hashing #############
my_id = get_hash(str(node_port))
print("My hashed id is: ", my_id)
#####################################



connect_ip =''
connect_port =0
s.bind((node_ip,node_port)) 
first_node = True



print("1- Start a new network or ") 
print("2 - Connect to an existing nework")
choice = input()

if(choice != str(1)):
	first_node = False
	#start connection protocol i.e send id 
	print("Enter Ip: " ); connect_ip = '127.0.0.1'
	print("Enter Port: "); 
	connect_port = int(input())
	joining_via = get_hash(str(connect_port))
	print("joining_via ", joining_via)
	c.connect((connect_ip,connect_port))
	
	data_dic = {}
	#making dic with hashed id and sending it
	data_dic["type"] = "init"; data_dic["joining_id"] = str(my_id); data_dic["joining_port"] = str(node_port)
	data_dic["joining_via_id"] = str(joining_via)
	string_data = json.dumps(data_dic)+'\n'
	successor[1] = connect_port
	successor_id = get_hash(str(connect_port)) #For now set successor to joining via
	c.sendall(string_data.encode())
else:
	#if this is a new network, this is the first node and it is its own predecessor and sucessor
	successor = [node_ip,str(node_port)]
	predecessor = [node_ip, str(node_port)]
	successor_id = str(my_id)
	predecessor_id = str(my_id)
	finger_table["1"] = [successor_id, successor[1]]
	
#start listening indefinitely
s.listen()

start_new_thread(listening_thread,(c,))

def file_log(useless):
	global mypath
	global files
	global h_files
	global accessing_files
	global copies
	global h_copies
	
	#while (1):
	if not accessing_files:
		files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
		files.remove(thisfile)
		h_files = []
		copies = []
		h_copies = []
		to_remove = []
		for filename in files:
			if "_copy" in filename:
				to_remove.append(filename)
				actual_name = filename.replace("_copy",'')
				#print(actual_name)
				copies.append(actual_name)
				h_copies.append(get_hash(actual_name))
			elif "_down" in filename:
				to_remove.append(filename)
			elif "_down" not in filename:
				h_files.append(get_hash(filename))

		for name in to_remove:
			files.remove(name)
	#time.sleep(3)

#start_new_thread(file_log,(c,))
file_log(True)
####################################################### UI STUFF AND FILE HANDLING

pause = False
breakbool = False
def input_thread(uselesss):
	global pause
	while 1:
		if pause == False:
			indat = input()
			if indat == 'q':
				pause =True
			elif indat ==  'u':
				file_log(True)
			elif indat == 'l':
				graceful_leave()
				breakbool = True

start_new_thread(input_thread,(True,))


while(1):

	print()
	print(finger_table)
	print('MY  Successor: ',successor_id)
	print('My ID: ', str(my_id))
	print('MY Predecessor:, ', predecessor_id)
	
	print()
	
	print('Files:')
	print(files)
	print(h_files)
	print()
	print("Copies:")
	print(copies)
	print(h_copies)
	print()

	if pause:
		print("Enter file name to download: ")
		indat = input()
		start_new_thread(request_download,(str(indat),))
		pause = False

	if breakbool or not ping_succ:
		break;
	#print("fromsucc_queue: ", fromsucc_queue)
	#print()
	time.sleep(3)
	system('clear')


for i in range(0, 10):
	print("exiting in", 10-i)
	time.sleep(1)

print("Shutting down")