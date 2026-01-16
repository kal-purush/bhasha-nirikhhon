def input_lines():
    try:
        while True:
            line = input()
            if not line:
                break
            yield line
    except KeyboardInterrupt:
        return
source_code = list(input_lines()) #user input
'''
source_code = ['svar a;',
               'a = 44;',
               'svar b;',
               'b = 31;',
               'svar str;',
               'str = "hello";',
               'print str;',
               'print a + b;']
'''

errorS, lno = 0, 0
for i in source_code: #semicolon misplaced
    lno += 1
    if i[len(i)-1] != ';':
        print('Error! at line ', lno)
        errorS = 1
        break

source_code2 = [] #formated source code
for i in source_code:
    ls = list(i)
    if ls[len(ls)-1] == ';':
        ls[len(ls)-1] = ' ;'
        source_code2.append(''.join(ls))

lexemes = []
for i in source_code2:
    tmp = i.split()
    lexemes.append(tmp)

error2=0
keywords = ['svar', 'print']
if lexemes[0][0] not in keywords:
    error2=1

literals = dict()
iden, cntr = 'z', 1
errorL = 0
if error2 != 1 and errorS != 1:
    for i in range(0, len(lexemes)):
        if lexemes[i][0] == 'svar':
            literals[lexemes[i][1]] = 0
            
    for i in range(0, len(lexemes)):
        if lexemes[i][1] == '=':
            if lexemes[i][0] in literals:
                literals[lexemes[i][0]] = lexemes[i][2]
            else:
                print('error')
                break

    for i in range(0, len(lexemes)):
        if lexemes[i][0] == 'print':
            if lexemes[i][2] == ';':
                xx = literals[lexemes[i][1]]
                print(xx[1:len(xx)-1])
            else:
                if lexemes[i][2] == '+':
                    try:
                        try:
                            xx = int(literals[lexemes[i][1]])
                            yy = int(literals[lexemes[i][3]])
                            print(xx+yy)
                        except ValueError:
                            xx = float(literals[lexemes[i][1]])
                            yy = float(literals[lexemes[i][3]])
                            print(xx+yy)                        
                    except TypeError:
                        print('unsupported operand')
                    except:
                        print('error')
                    
                if lexemes[i][2] == '-':
                    try:
                        try:
                            xx = int(literals[lexemes[i][1]])
                            yy = int(literals[lexemes[i][3]])
                            print(xx-yy)
                        except ValueError:
                            xx = float(literals[lexemes[i][1]])
                            yy = float(literals[lexemes[i][3]])
                            print(xx-yy)                            
                    except TypeError:
                        print('unsupported operand')
                    except:
                        print('error')
                    
                    
                if lexemes[i][2] == '*':
                    try:
                        try:
                            xx = int(literals[lexemes[i][1]])
                            yy = int(literals[lexemes[i][3]])
                            print(xx*yy)
                        except ValueError:
                            xx = float(literals[lexemes[i][1]])
                            yy = float(literals[lexemes[i][3]])
                            print(xx*yy)
                    except TypeError:
                        print('unsupported operand')
                    except:
                        print('error')
                        
                    
                if lexemes[i][2] == '/':
                    try:
                        try:
                            xx = int(literals[lexemes[i][1]])
                            yy = int(literals[lexemes[i][3]])
                            print(xx/yy)
                        except ValueError:
                            xx = float(literals[lexemes[i][1]])
                            yy = float(literals[lexemes[i][3]])
                            print(xx/yy)
                    except ZeroDivisionError:
                        print('infinity')
                    except TypeError:
                        print('unsupported operand')
                    except:
                        print('error')
else:
    print('error')

input('Enter key to exit')




        