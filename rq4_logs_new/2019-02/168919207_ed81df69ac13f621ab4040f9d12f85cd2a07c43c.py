import socket, sys, string 
 
# Python IRC Bot by c0deNinja aka s0urd (ninjabot v1.0)
# Python 3

if len(sys.argv) !=3:
	print ("Usage python ninjabot.py <server> <port> <channel>")
	sys.exit(1)

irc = sys.argv[0]
port = int(sys.argv[1])
chan = sys.argv[2].encode()
sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sck.connect((irc, port))
sck.send(b'NICK ninjabot\r\n')
sck.send(b'USER ninjabot ninjabot ninjabot :ninjabot Script\r\n')
sck.send(b'JOIN ' + b" " + chan + b'\r\n')
data = ''

arg = data.split( )   
args = b'' 
for index,item in enumerate(arg): 
	if index > 3: 
		if args == b'': 
			args = item 
		else : 
			args += b' ' + item
while True:
	data = sck.recv(1024)
	if data.find(b'PING') != -1:
		sck.send(b'PONG ' + data.split() [1] + b'\r\n')
		print (data)
	elif data.find(b'!info') != -1:
		sck.send(b'PRIVMSG ' + chan + b' :' + b' ninjabot v1.0 by c0deninja ' + b'\r\n')
		print (data)
	elif data.find(b'!commands') != -1:
			sck.send(b'PRIVMSG ' + chan + b' :' + b' OP' + b' DEOP' + b' KICK' + b'\r\n')
			print (data)
	elif data.find(b'PRIVMSG') != -1:
		message = b':'.join(data.split(b':')[2:])
		if message.lower().find(b'An0nr00t') == -1: 
			nick = data.split(b'!')[ 0 ].replace(b':',b' ')
			destination = b''.join (data.split(b':')[:2]).split (b' ')[-2]
			function = message.split( )[0]
			print (nick + b' : ' + function)
			arg = data.split( )
print (data)