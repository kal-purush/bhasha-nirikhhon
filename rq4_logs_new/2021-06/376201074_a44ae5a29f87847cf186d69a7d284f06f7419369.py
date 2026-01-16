from tokens import *

import os


class writeUml:

    def __init__(self,sourceCode):
        self.classDict = sourceCode
        self.file = 'web/UML.txt'


    def writeFile(self):
        file = open(self.file,'w')
        file.write("title My code\nscale 2\n")
        for classname in self.classDict:
            Class = "class "+classname+" {"
            file.write(Class+'\n')
            variables = self.classDict[classname]['class Variables']
            for var in variables:
                datatype,var = var.split()
                variable = "\t+ "+datatype+" : "+var
                file.write(variable+"\n")
            methods = self.classDict[classname]['class methods']
            for method in methods:
                datatype,var = method.split()
                method = "\t+ "+datatype+" : "+var
                file.write(method+"\n")
            file.write("}\n")
        for classname in self.classDict:
            if self.classDict[classname]['Inherited'] != "Base":
                Classname = self.classDict[classname]
                inherited = self.classDict[classname]['Inherited']
                inherit = inherited+" <|-- "+classname
                file.write(inherit+'\n')
        file.close()

    def run(self):
        #self.writeContent()
        self.writeFile()
        os.system("python -m plantuml web/UML.txt")

###################################################
# {
#   class :       
#            { 
#               classname : student
#               Inhertited : Base
#               classVariables :
#                               {    
#                                   string : usn
#                                   string : name
#                                   string : branch
#                                   long : phone
#                               }   
#               classMethods : 
#                             { 
#                                void : read(void)
#                                void : print(void)
#                             } 
#            }
#   }
############################################################