import sys
import socket
import cPickle as pickle
import threading


routingTable = {}
neighborDV = {}
distanceVector = {}
neighbors = []
s = 0

def ReadFile(config_file):
	''' Reads configuration file '''

	firstLine = config_file.readline()
	myconfig = firstLine.split()

	port = int(myconfig[0])
	timeout = int(myconfig[1])

	for line in config_file:
		info = line.split()
		neighbor = info[0]
		neighbor_distance = float(info[1])
		distanceVector[neighbor] = neighbor_distance

		# Initialize routing table (tuple with distance and next hop)
		routingTable[neighbor] = (neighbor_distance, neighbor)

	return (port, timeout)

def sendRT():
	''' Sends Routing table to neighbors '''
	for client in neighbors:
		clientAddr, clientPort = client.split(':')
		s.sendto("update")
		s.sendto(pickle.dumps(routingTable), (clientAddr,int(clientPort)))

def executeTimeout(timeout):
	sendRT()
	threading.Timer(timeout, executeTimeout, [timeout]).start()

def getRT():
	
	while(true):
		data, addr = sock.recvfrom(1024)
		new_rt = pickle.loads(data)


def main(argv):

	distanceVector = {}

	host = socket.gethostbyname(socket.gethostname())

	# Read the config file
	nameFile = sys.argv[1]
	config = open(nameFile)
	resultsFile = ReadFile(config)
	port = resultsFile[0]
	timeout = resultsFile[1]

	# Create a socket
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.bind((host, port))
	
	print "Host: " + host + "Port: " + str(port)
	print (">"),

	executeTimeout(timeout)

	threading.Thread(target = getRT)
    # Get commands
	while(True):

		commandLine = sys.stdin.readline()

		command_s = commandLine.split(' ')

		command = command_s[0].rstrip()

		if(len(command_s) == 1):
			parameters = ''
		
		else:
			parameters = command_s[1]
		

		if (command == "SHOWRT"):
			for destination in routingTable:
				print "Destination: " + destination + ", Cost: " + str(routingTable[destination][0]) + ", Link: " + routingTable[destination][1] + "\n",

main(sys.argv)