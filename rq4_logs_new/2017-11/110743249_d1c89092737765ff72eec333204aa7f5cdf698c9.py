import sys
from inspect import signature

class CommandData:
    
    def __init__(self,prompt,options,manual):
        self.options = options 
        self.prompt = prompt
        self.help = manual

    def execute(self,option,*params):
        self.options[option](*params)

    def getParams(self,option):
       return signature(self.options[option]).parameters

    def createPrompt(self):
        command = ""
        userWantsToExit = False
        while (not userWantsToExit):
            sys.stdout.write(self.prompt)
            command = input()
            commands = command.split(" ")

            option = commands[0]


            if (commands[0] in self.options):

                if(isValidCommand(data,commands)):
                    if(len(commands) > 1):

                        if functionHasTypes(self.getParams(option)):
                            types = getTypes(self.getParams(option))
                            commands = fixTypes(commands, types)

                        params = tuple(commands[1:len(commands)])
                        self.execute(commands[0],*params)

                    else:
                        self.execute(option)
                else:
                    print("[GENERAL] Invalid use of command.")

            elif(command == "help"):
                print(data.help)
            elif(command == "exit" or command == "quit"):
                userWantsToExit = True
                print("Goodbye.")
            elif(command == ""):
                None
            else:
                print("Unrecognized command.")


def functionHasTypes(metadata):
    if len(str(metadata).split(":")) == 1:
        return False
    return True

def checkTypes(types, commands):
    i = 0;
    hasError = False
    for t in types:
        try:
            if t == "int":
                int(commands[i])
            elif t == "float":
                float(commands[i])
        except ValueError:
            print("[TYPE] Incorrect use of types.")
            print("[TYPE] param " + str(i+1) + " should be of type " + t + ".")
            hasError = True
        i += 1
    if hasError:
        return False
    else:
        return True

def getTypes(params):
    types = []
    
    for param in params:
        types.append(str(params[param]).split(":")[-1:][0])
    return types

def fixTypes(commands, types):

    fixed = []
    fixed.append(commands[0])

    print(commands, types)
    for i in range(1,len(commands)):
        if types[i-1] == "int":
            print(i)
            fixed.append(int(commands[i]))
            print(fixed)
        elif types[i-1] == "float":
            fixed.append(float(commands[i]))
        else:
            fixed.append(str(commands[i]))
        
    return fixed 

def isValidCommand(data,commands):
    params = data.getParams(commands[0])

    if(len(params) == len(commands)-1):
        if (functionHasTypes(params)):
            types = getTypes(params)
            if checkTypes(types, commands[1:len(commands)]) == False:
                return False
            else:
                return True
        else:
            return True

    else:
        print("[USAGE] Invalid number of parameters.")
    return False


def createPrompt(data):
    command = ""
    while (command != "exit"):
        sys.stdout.write(data.prompt)
        command = input()
        commands = command.split(" ")

        option = commands[0]


        if (commands[0] in data.options):

            if(isValidCommand(data,commands)):
                if(len(commands) > 1):

                    if functionHasTypes(data.getParams(option)):
                        types = getTypes(data.getParams(option))
                        commands = fixTypes(commands, types)

                    params = tuple(commands[1:len(commands)])
                    data.execute(commands[0],*params)

                else:
                    data.execute(option)
            else:
                print("[GENERAL] Invalid use of command.")

        elif(command == "help"):
            print(data.help)
        elif(command == "exit" or command == "quit"):
            print("Goodbye.")
        elif(command == ""):
            None
        else:
            print("Unrecognized command.")

            

def show(string):
    print(string)

def length(string):
    print(len(string))

def add(a:int ,b:int):
    print (str(a) + " + " + str(b) + " = " + str(a+b))

def name_age(name:str, age:int):
    print("your name is " + name + " and you will be " + str(age+20) + " in 20 years if you aren't dead.") 

options = {"show":show,"length":length, "add":add, "name_age":name_age}

prompt = "Enter a command >>"

manual = "show <str>, length <str>, add <int> <int>, name_age <str> <int>"
        
data = CommandData(prompt,options,manual)


data.createPrompt()