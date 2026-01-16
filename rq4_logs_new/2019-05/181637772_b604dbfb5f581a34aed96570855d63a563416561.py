import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import pcapy
import math
from threading import Thread, Lock
import threading
MaxThreads = 20
mutex = Lock()
def treatData(sendID,sendTime,rID,rTime, dataPath, saveplace):
    print "yessir"
    delayList = []
    for i in range(len(sendID)):
	    for j in range(len(rID)):
		    if sendID[int(i)] == rID[int(j)]:
			    delayList.append(rTime[int(j)] - sendTime[int(i)])
    endToEndDelay = np.mean(delayList)
    #print("packets sent: {}\npackets received: {}".format(len(sendID),len(rID)))
    dropRate = round(((1-len(rID)*1.0/len(sendID))*100.0))
    #print("Droprate: {}".format(dropRate))
    #print("Average end-to-end delay is: {}".format(endToEndDelay))
    #samletDat = AltDataSendt(dataPath)
    samletDat = 1
    Overhead = samletDat
    if len(rID) != 0:
        Overhead = (samletDat - (len(rID)*1000))/(len(rID)*1000)*100
    print("Overhead in percent %d" %Overhead)
    mutex.acquire()
    try:
        f = open("%s/Collected_data.txt" %saveplace, 'a')
        f.write("\nChecking file %s\n" %dataPath)
        print dataPath
        print saveplace
        print len(rID)
        f.write("Overhead in percent is %d\n"% (Overhead))
        if len(rID) != 0:
            f.write("End to end delay is %d in nanoseconds\n" %endToEndDelay)
        f.write("packets sent %d\npackets received %d\n"%(len(sendID), len(rID)))
        f.close()
    finally:
        mutex.release()

    

def getData (dataPath, saveplace):
    sendingID = []
    sendingTime = []
    receivedID = []
    receivedTime = []

    myFile = open("{}/SendingLogdata.txt".format(dataPath),"r")
    for line in myFile:
        fields = line.split(", ")
        if fields[1].isdigit():
            sendingID.append(fields[1]) 
        sumvar = fields[3]
        sumvar = sumvar[1:-3]
        sendingTime.append(sumvar)
    myFile.close()

    sendingID = list(map(int, sendingID))
    sendingTime = list(map(float, sendingTime))
    sendingTime = list(map(int, sendingTime))
    if os.path.isfile("{}/ReceivedLogdata.txt".format(dataPath)) == True:
        myFile = open("{}/ReceivedLogdata.txt".format(dataPath),"r")
        for line in myFile:
            fields = line.split(", ")
            if fields[1].isdigit():
                receivedID.append(fields[1]) 
            sumvar = fields[3]
            sumvar = sumvar[1:-3]
            receivedTime.append(sumvar)
        myFile.close()

    receivedID = list(map(int, receivedID))
    receivedTime = list(map(float, receivedTime))
    receivedTime = list(map(int, receivedTime))

    #print("Sending ID: {} \n".format(sendingID))
    #print("Sending time: {} \n".format(sendingTime))

    #print("Receiving ID: {} \n".format(receivedID))
    #print("Receiving time: {} \n".format(receivedTime))
    
    treatData(sendingID,sendingTime,receivedID,receivedTime, dataPath, saveplace)

def AltDataSendt(path):
    File_list = []
    PcapFiles =[]
    Overheadsum = 0
    Samlet = 0
 #Vi finder alle Pcap filer i den folderen vi er i
    substring = '.pcap'
    for Allfiles in os.popen('ls %s' %path):
        if substring in Allfiles:
            PcapFiles.append(Allfiles) 
 #Vi kan nu finde alt data sendt som er i pcap filen.
    for pcap in PcapFiles:
 #We read one pcap file at a time.
        fil = path + "/" + pcap.rstrip()
        reader = pcapy.open_offline("%s" % fil)
 #Vi finder hvor mange frames der er i den givne pcap filen
        Lenght = os.popen('tshark -r %s | wc -l' % fil).read()
        PcapLenght = int(Lenght)
  #Vi koerer igennem alle frames/pakker og finder deres laengder i bytes
        for x in range(0, PcapLenght):
            (header, payload) = reader.next();
      # Summer alle frames laengder
      
            Overheadsum = Overheadsum + header.getlen();
    Samlet = Samlet + Overheadsum
    #print(Samlet)
    return(Overheadsum)

def main():
    #for Protocol in Protocols:
        Protocol = "OLSR"
        for test_typer_count in os.walk("%s" % (Protocol)).next()[1]:
            if os.path.isdir('%s/%s' % (Protocol, test_typer_count )) == True:
                for statistic_test_count in os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1]:
                    dir_navn = Protocol + "/" + test_typer_count
                    #print(os.walk("%s/%s"%(Protocol, test_typer_count)).next()[1])
                    Processedtest = dir_navn+ "/" + statistic_test_count
                    while True:
                            #We call the if to ensure no more than the max allowed threads are running at once
                        if threading.activeCount() < MaxThreads:
                            x = Thread(target = getData, args = (Processedtest, dir_navn,))
                            x.start()
                            #Break out of the while loop to return to the above for loop.
                            break



main()