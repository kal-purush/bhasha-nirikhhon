# this program takes fermi data as outputted by fv, and converts it into a 2D array of values:
# the 2D array consists of a list of lists, each containing E(MeV), RA, and DEC
# the data may then be outputted via xls or to plaintext
# this should make it easier to work with this data in Python (astropy) and JS (astrojs)
#

# !!!!!MAKE SURE TO CREATE FILES NAMED EITHER fermix.xls OR fermit.txt BEFORE RUNNING!!!!!! 


import xlwt
class fermiConverter():
    
    eventsT = ""
    events = []
    eventstr = ""
    fevents = [[]for n in range(0)]
    def __init__(self):
        pass
    
    def convertFile(self, file):
        self.eventsT = list(open(file, "r").read())
        for val in range(100000):
            if self.eventsT[val] == ",":
                self.eventsT[val] = ""
        for u in range(100000):
            self.eventstr += self.eventsT[u]
        self.events = self.eventstr.split('"')
        td = []
        for t in range(len(self.events)):
            
            if self.events[t] == "":
                td += [t]
                
            elif self.events[t] == " ":
                td += [t]
            elif self.events[t] == "\n":
                td += [t]
        o = 0
        for j in range(len(td)):
            del(self.events[td[j] - o])
            o += 1
        
        
                
        self.fevents = [[]for n in range(0)]
        nevents = []
        for v in range(int(int(len(self.events)) / 3)):
            for y in range(3):
                
                nevents += [self.events[3 * v + y]]
                print(nevents)
            self.fevents.append(nevents)
            nevents = []
            
        
        return self.fevents
                
    def output(self, typ):
        if typ == "xls":
            
            book = xlwt.Workbook()
            sh = book.add_sheet("fermix")

           

            col1_name = 'E (MeV)'
            col2_name = 'RA'
            col3_name = 'DEC'

            #You may need to group the variables together
            #for n, (v_desc, v) in enumerate(zip(desc, variables)):
            
            notation = int(input("enter 1 for scientific notation\nenter 2 for decimal notation"))
            
            if notation == 1:
                for n in range(len(self.fevents)):
                
                    for k in range(len(self.fevents[n])):
                        if "+" in self.fevents[n][k] and "0" in self.fevents[n][k]:
                            sh.write(n,k, self.fevents[n][k])
                        
            elif notation == 2:
                for n in range(len(self.fevents)):
                
                    for k in range(len(self.fevents[n])):
                        if "+" in self.fevents[n][k] and "0" in self.fevents[n][k]:
                            sh.write(n,k, float(self.fevents[n][k]))

            

            
            print("done")
            book.save("fermix.xls")
            
        elif typ == "txt":
            
            textfile = open("fermit.txt", "w")
            
            notation = int(input("enter 1 for scientific notation\nenter 2 for decimal notation"))
            if notation == 1:
                for d in range(len(self.fevents)):
                    for w in range(len(self.fevents[d])):
                    
                        textfile.write(str(self.fevents[d][w]) + " ")
                    textfile.write("\n")
            elif notation == 2:
                for d in range(len(self.fevents)):
                    for w in range(len(self.fevents[d])):
                    
                        textfile.write(str(float(self.fevents[d][w])) + " ")
                    textfile.write("\n")
            textfile.close()
fermi = fermiConverter()
events = fermi.convertFile((input("file name:") + ".txt"))
for n in range(30):
    for k in range(3):
        #print(events[n][k])
        pass
        
        
output = fermi.output(input("output format (txt or xls):"))