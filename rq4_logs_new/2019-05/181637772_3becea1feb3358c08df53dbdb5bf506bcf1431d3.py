import os
import re
import math
import array 
Protocols = ['DSDV']
#Define distance allowed to assume connection with gain=-10 should be 580

#Function generates a matrix of connections and returns what nodes the Gateway is connected to.
#The function takes a specific path to the test run such as path could be "AODV/AODV5/test5"
def Matrixgenerator(dirnavn, Distance):
    Filename = dirnavn +"/time"
    for files in os.walk(Filename):
    # Going through each file in the directory here
        print dirnavn
        for fil in files[2]:
            #We read a file with an array of what nodes theoretically should be in communication distance
            f=open('%s/%s'%(Filename,fil),'r')
            content = f.readlines()
            
            f.close()
            ID = []
            Xcoords =[]
            Ycoords = []
            for line in range(len(content)):
                Realcontent=[]
                #Removing newlines and whitespace
                Realcontent.append(content[int(line)].rstrip())
                #We now have a string such as nodeID,Xcoord,Ycoord (Realcontent) so this is split between each comma
                for stuffs in Realcontent:
                    entry = re.split(',',stuffs)
                    ID.append(entry[0])
                    Xcoords.append(entry[1])
                    Ycoords.append(entry[2])
            Connectivitymatrix = []
#In this code below the distance between nodes is checked and when the distance is less than the Distance variable defined previously
#The node will be set as "available" for communication formula for distane is sqrt((Xi - Xj)^2 + (Yi - Yj)^2)
#Where Xi, Yi and Xj, Yj are the coordinates for a specific node at the time.
#In this case for a specific row Xi,Yi is defined and compared to all other entries on that row to check if they can communicate.
#This will go on untill all nodes have been checked through.

#after running this through a symmetric matrix is created which will indicate what nodes are connected.
#Where all nodes are conencted to themselves

            for i in ID:
                Connectivityrow = []
                for k in range(len(ID)):
                    Connectivityrow.append(0)
                for j in ID:
                    distance = math.sqrt((float(Xcoords[int(i)]) - float(Xcoords[int(j)]))**2 + (float(Ycoords[int(i)]) - float(Ycoords[int(j)]))**2)
                    if distance < Distance:
                        Connectivityrow[int(j)] = 1
                    else:
                        Connectivityrow[int(j)] = 0
                Connectivitymatrix.append(Connectivityrow)
            if os.path.isdir('%s/Matrix' % (dirnavn)) != True:
                os.makedirs('%s/Matrix' % (dirnavn))
            l = open('%s/Matrix/connectivitymatrix%s.txt' % (dirnavn,fil),'w')
            for row in Connectivitymatrix:
                #print row
                string = str(row)[1:-1]
                l.write('%s\n' %string)
            l.close()
                    

#Simple way to automatically call the MatrixGenerator for all different simulations run
def Generate_Neighbourmatrix():
    for Protocol in Protocols:
        #Protocols here are AODV, OLSR, DSR, DSDV which are defined previously
    #    Protocol="OLSR"
        for test_typer_count in os.walk("%s" % (Protocol)).next()[1]:
            Distance = 290
            
            #test_typer_count are different types of test such as 5nodes,10nodes,20nodes defined when running automatic.py
            if os.path.isdir('%s/%s' % (Protocol, test_typer_count )) == True:
                for statistic_test_count in os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1]: 
                #statistic_test_count is the times a simulation with specific paramters as previously defined are run through to make statistics on them.
                    dir_navn = Protocol + "/" + test_typer_count + "/" + statistic_test_count
                    Matrixgenerator(dir_navn, Distance)
    



#This function takes the results from Matrixgenerator and check how well connected the network is
def connectivity(dirsti, fil):
    if fil != "connectionlevel.txt":
        
    #First a specific matrix is read and will be processed
        A=[]
        with open('%s/%s'%(dirsti,fil),'r') as f:
            lines = f.readlines()
            for line in lines:
                linje = line.rstrip()
                A.append(linje.split(","))
        nodetjek = []
        for entry in range(len(A)):
            nodetjek.append(1)
        nodetjek[0] = 0
        connection = A[0]
        #print dirsti
        #print fil
    
        for x in range(len(connection)):
            if connection[x] !="0.000000":
                connection[x] = float(connection[x])

#Example of how an array may look and the logic
#                   1,0,0,1,0
#                   0,1,1,1,0 
#                   0,1,1,0,0
#                   1,1,0,1,1
#                   0,0,0,1,1
#On the first row a check is done for what neighbours the gateway has. As it has the neighbour 4 it may add 4's neighbour list to its own which means the first row is now:
#                   2,1,0,2,1   Which means it now can also add node 2 and 5's neighbour to its own. This will go on until all neighbour's lists are added
#After adding all possible neighbours all numbers that aren't 0 will be set to 1 and afterwards summed up and divided by the amount of nodes
#As this will define the how much well connected the network is. In order to ensure that a network with only node 1 part of it doesn't have
#a connectivity level 1 is substracted from the sum and from the amount of nodes which will be divided by.
#The nodetjek array previously mentioned is to ensure that a nodes neighbour list is only added once
#to ensure infinite loop won't occur the "tjekker" variable is defined 
        tjekker = True
        while tjekker == True:
            tjekker=False
            for entry in range(len(connection)):
                if nodetjek[int(entry)] == 1 and connection[int(entry)] > 0:
                    tjekker=True
                    row = []
                    row = A[int(entry)]
                    for x in range(len(connection)):
                        value = A[int(entry)]
                        connection[x] = float(connection[x]) + float(value[x])
                    nodetjek[int(entry)] = 0
                    break
        for entry in range(len(connection)):
            if int(connection[entry])!= 0:
                connection[entry] = 1
        #print ("Vi er %s/%s "%(dirsti,fil))
        #print connection
        connectionlevel = ((sum(connection))*1.0/(len(connection))*1.0)
        f = open("%s/connectionlevel.txt"% dirsti,'a')
        f.write("%f,"%connectionlevel)
        f.close()
    
    

#comments applied to the Generate_Neighbourmatrix() are also applied here
#This function will call the connectivity function for all different simulations
#It has to be called after Generate_Neighbourmatrix() function
def Generator_Connectivity():
    for Protocol in Protocols:
        for test_typer_count in os.walk("%s" % (Protocol)).next()[1]:
            if os.path.isdir('%s/%s' % (Protocol, test_typer_count )) == True:
                for statistic_test_count in os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1]: 
                    dir_navn = Protocol + "/" + test_typer_count + "/" + statistic_test_count + "/Matrix"
                    for fil in os.walk("%s/%s/%s/Matrix"%(Protocol, test_typer_count, statistic_test_count)).next()[2]:
                        connectivity(dir_navn,fil)





#function which will collect information generated by the other functions
#It will average out the connectivity level for a simulation type i.e 5nodes
#This i done by averaging the connectivity of a single simulation
#After which it will averaged for all simulation using the same parameters such as 5nodes, 10 nodes and different routing
#routing should however not affect this at all but it is still calculated.
def Connecitvitysamler():
    for Protocol in Protocols:
        for test_typer_count in os.walk("%s" % (Protocol)).next()[1]:
            Samlet = 0
            if os.path.isdir('%s/%s' % (Protocol, test_typer_count )) == True:
                for statistic_test_count in os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1]: 
                    Totalconnectivity = 0
                    dir_navn = Protocol + "/" + test_typer_count + "/" + statistic_test_count + "/Matrix"
                    connectivityprsimulation = 0
                    #We find a directory such as "AODV/AODV5/test5/Matrix" which contains connectionlevel.txt
                    #this file contains all the diffrent connectivity levels throughout the simulation.
                    for fil in os.walk("%s/%s/%s/Matrix" % (Protocol, test_typer_count, statistic_test_count)).next()[2]:
                        if fil == "connectionlevel.txt":
                            content=open("%s/%s/%s/Matrix/connectionlevel.txt" % (Protocol, test_typer_count, statistic_test_count)).read()
                            datacontent = re.split(',', content)
                            totalval = 0
                            #We loop through the file and read all the values and add them together and average them
                            for i in range(len(datacontent)-1):
                                #Speciel case where 0.00000 is problematic to work with 
                                if datacontent[i] != "0.000000":
                                    totalval = float(totalval) +float(datacontent[i])
                            #After running an one of the connectionlevel.txt files through its averaged using the amount of entries
                            connectivityprsimulation = totalval/(len(datacontent)-1)
                            #This is added to "samlet" which will contain all values using same parameters
                            Samlet = Samlet + connectivityprsimulation
                #Average connectivity for simulaton with same parameters
                AverageConnectivity = Samlet/len(os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1])
                print "%s/%s"%(Protocol,test_typer_count)
                print AverageConnectivity
                ConnectivityVariance =0
                #Calculate variance
                for statistic_test_count in os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1]: 
                    Totalconnectivity = 0
                    dir_navn = Protocol + "/" + test_typer_count + "/" + statistic_test_count + "/Matrix"
                    connectivityprsimulation = 0
                    for fil in os.walk("%s/%s/%s/Matrix" % (Protocol, test_typer_count, statistic_test_count)).next()[2]:
                        if fil == "connectionlevel.txt":
                            content=open("%s/%s/%s/Matrix/connectionlevel.txt" % (Protocol, test_typer_count, statistic_test_count)).read()
                            datacontent = re.split(',', content)
                            totalval = 0
                            for i in range(len(datacontent)-1):
                                if datacontent[i] != "0.000000":
                                    totalval = float(totalval) +float(datacontent[i])
                            #After running an one of the connectionlevel.txt files through its averaged using the amount of entries
                            connectivityprsimulation = totalval/(len(datacontent)-1)
                            #This is added to "samlet" which will contain all values using same parameters
                            connectivityprsimulation
                            ConnectivityVariance = ConnectivityVariance + (AverageConnectivity - connectivityprsimulation)*1.0 * (AverageConnectivity - connectivityprsimulation)*1.0
                ConnectivityVariance = ConnectivityVariance*1.0/len(os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1])
                Connectivity_Standard_deviation = math.sqrt(ConnectivityVariance)
                Confidence_Connectivity_left = round(AverageConnectivity - (1.96 * Connectivity_Standard_deviation/math.sqrt(len(os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1]))),5)
                Confidence_Connectivity_right =round(AverageConnectivity + (1.96 * Connectivity_Standard_deviation/math.sqrt(len(os.walk("%s/%s" % ( Protocol, test_typer_count)).next()[1]))),5)
                print Confidence_Connectivity_left
                print Confidence_Connectivity_right
                
                
                
                f = open("Histogramdata/Intervaller/AverageConnectivity.txt",'a')
                f.write("%f,"%AverageConnectivity)
                f.close()
                
                f = open("Histogramdata/Intervaller/Confidence_Connectivity_left.txt",'a')
                f.write("%f,"%Confidence_Connectivity_left)
                f.close()
                
                f = open("Histogramdata/Intervaller/Confidence_Connectivity_right.txt",'a')
                f.write("%f,"%Confidence_Connectivity_right)
                f.close()


Generate_Neighbourmatrix()
Generator_Connectivity()
Connecitvitysamler()














