from socket import *
import pickle 
from _thread import *

import sys
sys.path.append('../')
from p7_simsearch import *


def clientThread(conn, search_engine):
	while True:
		rec = conn.recv(1024)
        if not rec:
        	break

        recv_data = pickle.loads(rec)
        if recv_data['cmd'] == "SIM_TS":
        	ts = Timeseries(recv_data['value'], recv_data['time'])
        	res = search_engine.search(ts, recv_data['n'])
        	conn.send(pickle,dumps(res))
        else:
        	break

    conn.close()



if __name__ == "__main__":
	search_engine = find_closest()
	s = socket(AF_INET, SOCK_STREAM)
	HOST = gethostname()
	PORT = 15001
	socket_s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
	s.bind((HOST, PORT))
    s.listen(5)
    try:
    	while True:
			conn, addr = s.accept()
			print('Connected by', addr)
			t = threading.Thread(target=clientThread,args=(conn, search_engine))
			t.start()
	finally:
		s.close()