import flowtbag
from flow import features as f
import Queue
from scapy.all import *
import threading
import MySQLdb

nomeBase = "../../gta/dataset-mobile/dump.pcap"
totalDeArquivos = 13285
janela = 2.
time0 = 0
cdDataset = 1


continuar = True
times = {}
PKTS = []
BUFFER = Queue.Queue()

ENCAPSULAMENTO = PPPoE

db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="dataset",         # your username
                     passwd="gta-dataset",  # your password
                     db="dataset")        # name of the data base
cur = db.cursor()

insert = "INSERT INTO `Fluxos` (`Cd_Dataset`, `Time`, `Timestamp`, `srcip`, `srcport`, `dstip`, `dstport`, `proto`, `total_fpackets`, `total_fvolume`, `total_bpackets`, `total_bvolume`, `min_fpktl`, `mean_fpktl`, `max_fpktl`, `std_fpktl`, `min_bpktl`, `mean_bpktl`, `max_bpktl`, `std_bpktl`, `min_fiat`, `mean_fiat`, `max_fiat`, `std_fiat`, `min_biat`, `mean_biat`, `max_biat`, `std_biat`, `duration`, `min_active`, `mean_active`, `max_active`, `std_active`, `min_idle`, `mean_idle`, `max_idle`, `std_idle`, `sflow_fpackets`, `sflow_fbytes`, `sflow_bpackets`, `sflow_bbytes`, `fpsh_cnt`, `bpsh_cnt`, `furg_cnt`, `burg_cnt`, `total_fhlen`, `total_bhlen`, `dscp`, `class`) VALUES ('"+str(cdDataset)+"'"


#sufixos = [""] + [str(i) for i in range (1, totalDeArquivos)]
sufixos = [str(i) for i in range (144, totalDeArquivos)]


def lerDoBuffer():
	oldTimestamp = 0
	print "Comecei a thread de leitura"
	while continuar or not BUFFER.empty():
		pkts = BUFFER.get()
		flows=flowtbag.Flowtbag(pkts)
	
		for flow in flows.active_flows.values():
			f = str(flow).split(",")
			try:
				timestamp = times[(str(f[0]), int(f[1]),str(f[2]), int(f[3]))]
			except:
				timestamp = oldTimestamp
			oldTimestamp = timestamp

			dado = (timestamp, f)

			datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dado[0]))
			strInsert = insert + ", '" + str(timestamp) + "', '"+ str(datetime)+"', '"+dado[1][0]+"', '"+dado[1][1]+"', '"+dado[1][2]+"', '"+dado[1][3]
			for i in dado[1][4:]:
				strInsert = strInsert + "', '" + i
			
			strInsert = strInsert + "', '" + "0" + "');"

			cur.execute(strInsert)
			#print strInsert			

			

		cur.execute("Commit;")
		print "Fim do flowbag"
		print "BUFFER", BUFFER.qsize()


leitura = threading.Thread(target=lerDoBuffer)
leitura.start()

for sufixo in sufixos:
	filename = nomeBase+sufixo
	print "\n\n==============================="
	print "Tratando arquivo", filename
	print "===============================\n\n"
	packets = rdpcap(filename)


	if time0 == 0: time0 = packets[0].time

	for p in packets:
	    try:
		    if ENCAPSULAMENTO in p: p = p[ENCAPSULAMENTO][1]
		    if IP in p and TCP in p[IP]:
			a = (p[IP].src,p[TCP].sport,p[IP].dst,p[TCP].dport)
			if a not in times:
			    times[a] = p.time
		    elif IP in p and UDP in p[IP]:
			a = (p[IP].src,p[UDP].sport,p[IP].dst,p[UDP].dport)
			if a not in times:
			    times[a] = p.time
                    else:
                            continue
		    if p.time - time0 > janela:
			pkts= []
			for pkt in PKTS:
			    try:
                                pkts.append((len(pkt), str(pkt), pkt.time))
                            except RuntimeError:
                                pass
			pkts.sort(cmp=lambda x, y: int(1000000000*(x[2] - y[2])))
			BUFFER.put(pkts)
			PKTS = []
			time0 = p.time
		    PKTS.append(p)
	    except RuntimeError:
		    print "RuntimeError"
		    print filename
		    continue

continuar = False




