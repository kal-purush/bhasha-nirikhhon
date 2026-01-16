"""
@Program Name: Yo-Shell V1 Starter Code
@Author: Christopher James
@Description:
    This code is the barebones shell. All additional commands will be added
    through modules. 
    
    <ver. 0.8.843>
"""
import os

#1#################################
def cp(argument):
    
    try:
        filename1 = argument[0]
        filename2 = argument[1]
    except:
        print("Error in arguments")
        
    f1 = open(filename1,'r')
    f2 = open(filename2,'w')
    
    text = f1.read()
    
    f2.write(text)
##################################
 

#2#################################
def cat(argument):
    
    try:
        filename1 = argument[0]
    except:
        print("Error in arguments")
        
    f1 = open(filename1,'r')
    
    text = f1.read()
    
    print(text)
##################################   
    
    
#3#################################
def history(argument):
    return
##################################
   
   
#4#################################
def mv(argument):
    
    try:
        filename1 = argument[0]
        filename2 = argument[1]
    except:
        print("Error in arguments")
        
    os.rename(filename1,filename2)
##################################
    
    
#5#################################
def rm(argument):
    
    try:
        filename1 = argument[0]
    except:
        print("Error in arguments")
        
    os.remove(filename1)
##################################


#6#################################
def wc(argument):
    
    try:
        filename1 = argument[0]
    except:
        print("Error in arguments")
        
    f1 = open(filename1,'r')
    
    text = f1.read()
    text = text.split()
    
    
    print(len(text))
##################################
    
    
#7#################################
def chmod(argument):
    
    try:
        filename1 = argument[0]
        code = '0o'+ argument[1]
    except:
        print("Error in arguments")
        
    os.chmod(filename1, code)
##################################
    
    
#8#################################
def cd(argument):
    
    try:
        filename1 = argument[0]
    except:
        print("Error in arguments")
        
    os.chdir(filename1)
##################################
    
    
#9#################################
def ls():
    
    print(os.listdir(path='.'))
##################################
    
    



#MAIN########################################    
cmdList = {'cp': cp, 'cat':cat, 'history':history,
           'mv':mv,  'wc':wc,   'ls':ls, 
           'chmod':chmod,'cd':cd, 'rm':rm, }

cmd = "$"

while (cmd != "exit"):
    cmd = input("~$: ")
    
    command = cmd.split()
    
    if len(command) < 1:
       continue
    elif len(command) == 1:
        cmd = command[0]
        if cmd == "exit":
            continue
        #########
        try:
            cmdList[cmd]()
        except:
            print("Error: Command not found")
        #########
    else:
        cmd = command[0]
        argument = []
        
        for i in range(1,len(command)):
            argument.append(command[i])
            
        #########
        try:
            cmdList[cmd](argument)
        except:
            print("Error: Command not found or Argument mismatch")
        #########


#########################################