#Assembler

###criação de listas para ter os dados dos registradores, opcodes, functs de instruções tipo R###
register = {}
op = {}
functr = {}

###abrindo o arquivo em que o output em binário sairá###
codigo_binary = open("codigo_binario.txt", "w+")

###função para transformar int/dec em binário
def intBinario(value, size):
    binario = bin(value)[2:]
    return binario.zfill(size)

###função para encontrar as labels
def find(label, size):
    cont =0
    with open("example.asm") as f:
        for line in f:
            cont +=1
            instrucao = line.split()
            if label in instrucao[0]:
                return intBinario(cont, size)

#virificar se a instrução existe
def instExists(op, value):
    if value in op.keys():
        return 's'
#abre o arquivo com opcodes e armazena na lista
with open("OPCODE.txt") as f:
    for line in f:
        (key, val)  = line.split()
        op[key] = val
#abre o arquivo com registradores e armazena na lista
with open("registers.txt") as f:
    for line in f:
        (key, val)  = line.split()
        register[key] = val  
#abre o arquivo com functs das instruções tipo R e armazena na lista
with open("funct.txt") as f:
    for line in f:
        (key, val)  = line.split()
        functr[key] = val 
#abre o arquivo com código em assembly e armazena na lista
with open("example.asm") as f:
    for line in f:
        #trata virgulas
        line = line.replace(',',' ').replace('(',' ').replace(')',' ').split()
        
        ###remove comentários
        if ':' in line:
            del line[0]
            cont = 0
            for i in line:
                if ';' in i:
                    while len(line) != cont:
                        del line[cont]
                cont += 1

        #pega a instrução encontrada no começo da linha no split 0
        instrucao = instExists(op, line[0])
        
        if instrucao == 's':
            ###intruções branch
            if line[0] == 'beq' or line[0] == 'bgtz' or line[0] == 'blez' or line[0] == 'bne':
                linha = op[line[0]] + register[line[1]] + register[line[2]] + find(line[3]+':',16)
                codigo_binary.write(linha + '\n')
            ###instruções tipo I
            elif line[0] == 'lb' or line[0] == 'lbu' or line[0] == 'lh' or line[0] == 'lhu' or line[0] == 'lw' or line[0] == 'sb' or line[0] == 'sh' or line[0] == 'sw':
                linha = op[line[0]] + register[line[1]] + intBinario(int(line[2]), 16) + register[line[3]]
                codigo_binary.write(linha + '\n')
            ###intruções tipo J
            elif line[0] == 'j' or line[0] == 'jal' or line[0] == 'jalr' or line[0] == 'jr':
                if line[0] == 'jr':
                    linha = op[line[0]] + register[line[1]].zfill(26)
                    codigo_binary.write(linha + '\n')
                else:
                    linha = op[line[0]] + find(line[1]+':',16)
                    codigo_binary.write(linha + '\n')
            ###intruções tipo R
            elif line[0] == 'add' or line[0] == 'addu' or line[0] == 'addi' or line[0] == 'addiu' or line[0] == 'and' or line[0] == 'andi' or line[0] == 'div' or line[0] == 'divu' or line[0] == 'mult' or line[0] == 'multu' or line[0] == 'nor' or line[0] == 'or' or line[0] == 'ori' or line[0] == 'sll' or line[0] == 'sllv' or line[0] == 'sra' or line[0] == 'srav' or line[0] == 'srl' or line[0] == 'srlv' or line[0] == 'sub' or line[0] == 'subu' or line[0] == 'xor' or line[0] == 'xori' or line[0] == 'slt' or line[0] == 'slti' or line[0] == 'sltu' or line[0] == 'sltiu':
                if line[0][-1:] == 'i' or line[0][-2:] == 'iu': #com imd
                    linha = op[line[0]] + register[line[1]] + register[line[2]] + intBinario(int(line[3]),16)
                    codigo_binary.write(linha + '\n')
                else:
                    if line[0] == 'sll' or line[0] == 'srl' or line[0] == 'sra':
                        linha = op[line[0]] + '00000' + register[line[1]] + register[line[2]] + intBinario(int(line[3]), 5) + functr[line[0]]
                        codigo_binary.write(linha + '\n')
                    else:
                        linha = op[line[0]] + register[line[1]] + register[line[2]] + register[line[3]] + '00000' + functr[line[0]]
                        codigo_binary.write(linha + '\n')