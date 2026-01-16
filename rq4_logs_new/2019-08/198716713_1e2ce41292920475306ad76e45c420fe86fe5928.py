#Read the file with all the registers       
with open("registers.txt", 'r+') as registersobj:
    for lines in registersobj:
        registers = list(registersobj)

PC = 0

instruction_memory = []

def instructionfetch():
    #Read the file with all the instructions
    with open("Assignment5_input.txt", 'r') as instructionsobj:
        for lines in instructionsobj:
            instruction = list(instructionsobj) #store each line into a list called instruction_memory -> this will allow for easy indexing of each instruction
    for i in instruction:
        instruction_memory.append(i.strip())
    return instruction_memory[PC]
        
#print(type(instructionfetch()))
'''
Opcodes
ADDI = 0
ADD = 1 
SUBI = 2 
SUB = 3 
LDUR = 4 
STUR = 5 
CBZ = 6 
B = 7
'''
def instructiondecode(enter):
    decode = []
    if enter[0] == 'A':
        if enter[PC][3] == 'I':
            decode[0] = 0
            w = enter[PC][6]
            Rd = w + enter[PC][7]
            Rd = int(Rd)
            decode[1] = Rd
            x = enter[PC][11]
            Rn = x + enter[PC][12]
            Rn = int(Rn)
            decode[2] = Rn
            y = enter[PC][16]
            z = y + enter[PC][17]
            imm = z + enter[PC][18]
            imm = int(imm)
            decode[3] = imm
            decode[4] = 0
            decode[5] = 0
        elif enter[PC][3] == ' ':
            decode[0] = 1
            w = enter[PC][6]
            Rd = w + enter[PC][7]
            Rd = int(Rd)
            decode[1] = Rd
#------------------------------------------------------------#            
            x = enter[PC][11]
            Rn = x + enter[PC][12]
            Rn = int(Rn)
            decode[2] = Rn
#------------------------------------------------------------#            
            y = enter[PC][16]
            Rm = y + enter[PC][17]
            Rm = int(Rm)
            decode[5] = Rm
            decode[3] = 0
            decode[4] = 0
    if enter[PC][0] == 'S':
        if enter[PC][3] == 'I':
            
            

def execute():
    pass

def datamemory():
    pass

def writeback():
    pass

pipeline1 = " " #string
pipeline2 = [] #list of ints
pipeline3 = [] #list of ints
pipeline4 = []


while PC < 22:
    writeback(pipeline4)
    pipeline4 = datamemory(pipeline3)
    pipeline3 = execute(pipeline2)
    pipeline2 = instructiondecode(pipeline1)
    pipeline1 = instructionfetch()
    PC = PC + 1