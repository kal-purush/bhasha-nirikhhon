import os
import re
import math
Protocols = ['AODV', 'OLSR', 'DSR', 'DSDV']

def Matrixgenerator(dirnavn):
    Filename = dirnavn +"/time"

    for files in os.walk(Filename):

    # Going through each file in the directory here
        for fil in files[2]:
            #We read a specific file
            f=open('%s/%s'%(Filename,fil),'r')
            content = f.readlines()
            f.close()
            #After reading the file we write the content into a new variable without any newlines
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

            for i in ID:
                Connectivityrow = []
                for k in range(len(ID)):
                    Connectivityrow.append(0)
                for j in ID:
                    distance = math.sqrt((float(Xcoords[int(i)]) - float(Xcoords[int(j)]))**2 + (float(Ycoords[int(i)]) - float(Ycoords[int(j)]))**2)
                    #print ("X: %s - %s Y: %s - %s"%(Xcoords[int(i)],Xcoords[int(j)],Ycoords[int(i)],Ycoords[int(j)])) 
                    #print distance
                    #print ('%s,%s' %(i,j))
                    #print distance
                    if distance < 580:
                        Connectivityrow[int(j)] = 1
                    else:
                        Connectivityrow[int(j)] = 0
                Connectivitymatrix.append(Connectivityrow)
            if os.path.isdir('%s/Matrix' % (dirnavn)) != True:
                os.makedirs('%s/Matrix' % (dirnavn))
            l = open('%s/Matrix/%sconnectivitymatrix.txt' % (dirnavn,fil),'w')
            for row in Connectivitymatrix:
                string = str(row)[1:-1]
                l.write('%s\n' %string)
                    
                    


def main():


    for test_typer in os.walk('.').next()[1]:
        #Directory wise this is /ns-3-dev/Results/OLSR for example
        for test_typer_count in os.walk("%s" % (test_typer)).next()[1]:
            # Directory wise this is /ns-3-dev/Results/OLSR/OLSR5 where the last directory is the different tests as specified in automatic.py
            #Further down we now are in the directory of extra tests for the same test in order to make statistical analysis on it
            if os.path.isdir('%s/%s' % (test_typer, test_typer_count )) == True:
                for statistic_test_count in os.walk("%s/%s" % ( test_typer, test_typer_count)).next()[1]: 
                # Directory wise this is /ns-3-dev/Results/OLSR/OLSR5/test5 where the last directory will maximum be the run_number specified in automatic.py
                #We define the specific directory to look into for the thread.
                    dir_navn = test_typer + "/" + test_typer_count + "/" +statistic_test_count
                    Matrixgenerator(dir_navn)
                    print(dir_navn)
    

                
        
        
main()
    
















