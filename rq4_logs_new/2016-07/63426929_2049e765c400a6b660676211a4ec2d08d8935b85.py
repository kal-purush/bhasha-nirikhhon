

import os 
dirfile = "/afs/cern.ch/work/y/yamhis/gangadir/workspace/yamhis/LocalXML/26/"
print "will be searching in directory "
print dirfile 



filename = "stdout"

#thisdirfile = open(dirfile,'r')

total = 0
NumberOfJobs = 50
for id in range(0, NumberOfJobs):    
   	#print id    
       	
	stdout = dirfile+str(id)+"/output/stdout"
        #print stdout

        if (os.path.exists(stdout)):
         thisfile = open(stdout,'r') 		
	 for line in thisfile :
		 if line.find('DaVinciInitAlg                SUCCESS') > -1 and line.find('events processed') > -1 :
			total += int(line.split()[2])
	thisfile.close()

print "-----------------------------"
print str(total)+" Were processed ! "
