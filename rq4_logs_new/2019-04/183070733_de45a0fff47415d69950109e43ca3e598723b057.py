import socket
import time

#hub server to handle pairing pings from sensors
# TODO: SAVE FOR MAC ADDRESS ISSUE

class HubServer:
	def __init__(self):
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.address = ('0.0.0.0', 5678)
		self.queue = 1

	def start(self):
		""" Listens for connection from hub."""
		try:
			self.socket.bind(self.address)
			self.socket.listen(self.queue)
			print("Listening on {} port {}".format(*self.address))
			return True
		except:
			print("Unable to start connection with hub")
			return False

	def stop(self):
		"""Closes connection with hub."""
		self.socket.close()

	@staticmethod
	def cleanupSocket(s):
		try:
			s.close()
		except:
			pass

	@staticmethod
	def get_commands_from(conn, handler):
		while True:
			try:
				data = conn.recv(32)
			except:
				HubServer.cleanupSocket(conn)
				print("No command received. Closing connection.")
				break
			if data:
				command = data.decode('utf-8') #could UTF-8 decoding fail?
				print("Forwarding command to handler: " + command)
				try:
					response = handler.run_command(command)
				except Exception as e:
					response = bytes('','utf-8')#don't allow response to be uninitialized when sending back
					print("Exception in command handler")
					print(e)
				if response:
					try:
						# Ensure that handler has already encoded response to bytes
						conn.sendall(response)
					except:
						print("The sensor response failed to send. Closing connection.")
						HubServer.cleanupSocket(conn)
			else:
				print("No command received. NO DATA!")
				HubServer.cleanupSocket(conn)
				break
			time.sleep(0.25)

	def get_commands(self, handler):
		while True:
			try:
				conn, client_addr = self.socket.accept()
				print("New connection:: ", client_addr)
				HubServer.get_commands_from(conn,handler)
			except Exception as e:
				print("Unable to accept socket connection.")
				print(e)
				return False



class DummyHandler:

	def command_callback(self, conn, client_addr, data):
		print(str(data))
		return bytes("OK", "utf-8")


#response_handler = ResponseHandler()
#serv = SensorServer()
#serv.start()
#serv.get_commands(cmd_handler)
#serv.stop()
