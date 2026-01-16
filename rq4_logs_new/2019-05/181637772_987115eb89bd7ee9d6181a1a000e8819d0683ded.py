import os
import signal
import subprocess

def auto():
    #Test

        for i in range(40):
            RunNumber = i + 1
            dirnavn = "Results/OLSR/OLSR"+str(RunNumber)
            if os.path.isdir(dirnavn) != True:
                os.popen ('mkdir %s' % dirnavn)
            subprocess.Popen('./waf --run "scratch/bitchboi --Run_number=%d"' %RunNumber, shell=False)

            
        
        
        
def nodes():
    #Virker
    for i in range(10): # How many diffrent node amount we try times 5
        numNodes = (i + 1)*5 
        for j in range(20): # How many individual tests are run for each node amount
                RunNumber = j + 1
                dirnavn = "Results/OLSR/OLSR" + str(numNodes)+ "/" +"Test" +str(RunNumber)
                print(os.path.isdir(dirnavn))
                if os.path.isdir(dirnavn) != True:
                    os.makedirs (dirnavn)
                os.popen('./waf --run "scratch/bitchboi --numNodes=%d --Run_number=%d --File_name=%s"' %(numNodes, RunNumber, dirnavn))    
            #subprocess.Popen('./waf --run "scratch/bitchboi --Run_number=%d --numNodes=%d "' %(RunNumber, numNodes), shell=True)
            
def Datarate():
    #virker ikke endnu
    for i in range(10): # How many diffrent node amount we try times 5
        rate = str((i + 1) * 0.1) # The number here is how much we increase the datarate by for each iteration 
        for j in range(20): # How many individual tests are run for each node amount
            
            ##########################################Note til mig selv: Check errormessag for this config as it contains valid information ################################1##
                RunNumber = j + 1
                DataRate = ("DsssRate%sMbps" % rate)
                dirnavn = "Results/OLSR/OLSR" + str(rate)+ "/" +"Test" +str(RunNumber)
                print(os.path.isdir(dirnavn))
                if os.path.isdir(dirnavn) != True:
                    os.makedirs (dirnavn)
                os.popen('./waf --run "scratch/bitchboi --phyMode=%s --Run_number=%d --File_name=%s"' %(Datarate, RunNumber, dirnavn))    
            
def Packets():
    #virker?
    for i in range(10): # How many diffrent packet sizes we try times 5
        PacketSize = (i + 1) * 100 # This number represents how much the packet size is increased with each iteration
        for j in range(20): # How many individual tests are run for each node amount
                RunNumber = j + 1
                dirnavn = "Results/OLSR/OLSR" + str(PacketSize)+ "/" +"Test" +str(RunNumber)
                print(os.path.isdir(dirnavn))
                if os.path.isdir(dirnavn) != True:
                    os.makedirs (dirnavn)
                os.popen('./waf --run "scratch/bitchboi --packetSize=%d --Run_number=%d --File_name=%s"' %(PacketSize, RunNumber, dirnavn))    
#auto()
nodes()
#Datarate()
#Packets()


        
    


        
    