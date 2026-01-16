'''
An assembler for my Turing Complete game's LEG architecture.
This was made because the in-game assembler is kinda annoying to work with.

Labels will be one of the few things not modified by the assembler, so it's not really a full 'assembler'
but it's not a transpiler (transsembler?) either.

Programs will be output in line broken decimal for the most part.

Specification date: 10/21/2023
'''
import argparse

def assemble_line(line: str) -> str:
    '''
    Assemble the line of decimal code needed to execute this line.
    '''
    opcode = 0
    arg0 = "0"
    arg1 = "0"
    arg2 = "0"
    mode = 'invalid'
    tokens = line.upper().split()  
    if len(tokens) == 0:
        return ''
    match tokens[0]:
        case 'ADD':
            # opcode = 0
            mode = 'alu'
            pass
        case 'SUB':
            opcode = 1
            mode = 'alu'
        case 'AND':
            opcode = 2
            mode = 'alu'
        case 'OR':
            opcode = 3
            mode = 'alu'
        case 'NOT':
            opcode = 4
            mode = 'alu'
        case 'XOR':
            opcode = 5
            mode = 'alu'
        case 'MULTH':
            opcode = 6
            mode = 'alu'
        case 'MULTL':
            opcode = 7
            mode = 'alu'
        case 'BE':
            opcode = 0b100000
            mode = 'compare'
        case 'BN':
            opcode = 0b100001
            mode = 'compare'
        case 'BL':
            opcode = 0b100010
            mode = 'compare'
        case 'BLE':
            opcode = 0b100011
            mode = 'compare'
        case 'BG':
            opcode = 0b100100
            mode = 'compare'
        case 'BGE':
            opcode = 0b100101
            mode = 'compare'
        case 'CALL':
            opcode = 0b100110
            mode = 'call'
        case 'JUMP':
            opcode = 0b100000
            mode = 'call'
        case 'RET':
            opcode = 0b100111
            mode = 'noargs'
        case 'SAVE':
            opcode = 0b10000
            mode = 'save'    
        case 'LOAD':
            opcode = 0b11000
            mode = 'load'   
        case 'MOV':
            opcode = 3
            mode = 'move'   
        case '#':
            return ""
        case 'LABEL':
            return "label " + tokens[1]
    # Now that we've matched the token to its basic instruction,
    # we can read future lines
    match mode:
        case 'noargs':
            pass
        case 'alu':
            arg0 = get_reg_arg(tokens[1])
            if is_imm(tokens[1]):
                opcode |= 128
            arg1 = get_reg_arg(tokens[2])
            if is_imm(tokens[2]):
                opcode |= 64
            arg2 = get_reg_arg(tokens[3])
        case 'compare':
            arg0 = get_reg_arg(tokens[1])
            if is_imm(tokens[1]):
                opcode |= 128
            arg1 = get_reg_arg(tokens[2])
            if is_imm(tokens[2]):
                opcode |= 64
            arg2 = tokens[3]
        case 'call':
            arg2 = tokens[1]
        case 'save':
            arg0 = get_reg_arg(tokens[1])
            if is_imm(tokens[1]):
                opcode |= 128
        case 'load':
            arg2 = get_reg_arg(tokens[1])
        case 'move':
            arg0 = get_reg_arg(tokens[1])
            if is_imm(tokens[1]):
                opcode |= 128
            arg2 = get_reg_arg(tokens[2])
            opcode |= 64
        case default:
            print('Invalid instruction', tokens[0])
            exit(1)
    
    # Finally, output the line of Nearly Byte Code.
    return str(opcode) + ' ' + arg0 + ' ' + arg1 + ' ' + arg2
    
regs = {'R0': 0, 'R1': 1, 'R2': 2, 'R3': 3, 'R4': 4, 'R5': 5, 'ADDR': 5, 'PC': 6, 'IO': 7}

def is_imm(token) -> int:
    return not token in regs

def get_reg_arg(token) -> int:
    if token in regs:
        return str(regs[token])
    elif token.isdigit():
        return token
    else:
        print('Invalid argument', token)
        exit(1)

parser = argparse.ArgumentParser("legasm.py")
parser.add_argument('in', help="LEG ASM file to be assembled")
parser.add_argument('-o', '--out', type=str, help="filename to be produced, defaults to 'leg.out'", default='leg.out')
args = vars(parser.parse_args())

with open(args['in']) as infile:
    with open(args['out'], mode='w') as outfile:
        print("# Assembled with legasm.py", file=outfile)
        for line in infile:
            code = assemble_line(line)
            if not code == '':
                print(code, file=outfile)
        infile.seek(0)
        print(file=outfile)
        print("# Original:", file=outfile)
        for line in infile:
            if line.strip() == '':
                continue
            print('#', line, end='', file=outfile)