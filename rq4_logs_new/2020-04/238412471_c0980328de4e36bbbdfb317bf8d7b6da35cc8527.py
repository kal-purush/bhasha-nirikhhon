def not_exist_newdic(name_str, dic2):
    try:
        dic2[name_str]
        return False
    except:
        return True

def two_value_check(one_str, dic1, dic2, i):
    #one_v is one of the value inside of d1['value']
    try:
        int_v = int(one_str)
        bool_v = True
    except:
        if not_exist_newdic(one_str, dic2):
            i += 1
            int_v = "error"
            bool_v = False
            get_value(one_str, dic1, dic2, i)
        else:
            int_v = dic2[one_str]
            bool_v = True
   
    return int_v, bool_v

def get_value(name_str, dic1, dic2, i):
    
    d1 = dic1[name_str]
    
    if i <3:
        
        if d1['connect_method'] == 'eql':
            try:
                a = int(d1['value'])
                dic2.update({name_str: a})
            except:
                if not_exist_newdic(d1['value'], dic2):
                    i += 1
                    get_value(d1['value'], dic1, dic2, i)
                else:
                    dic2.update({name_str: dic2[d1['value']]})
        
        elif d1['connect_method'] == 'not':
            try:
                a = 0xffff - int(d1['value'])
                dic2.update({name_str: a})
            except:
                if not_exist_newdic(d1['value'], dic2):
                    i += 1
                    get_value(d1['value'], dic1, dic2, i)
                else:
                    a = 0xffff - dic2[d1['value']]
                    dic2.update({name_str: a})
                    
        else:
            A, B = d1['value']
            
            a, c = two_value_check(A, dic1, dic2, i)
                
            b, d = two_value_check(B, dic1, dic2, i)
                
            
            if c and d:
                if d1['connect_method'] == 'AND':
                    final_v = a & b
                elif d1['connect_method'] == 'OR':
                    final_v = a | b
                elif d1['connect_method'] == 'LSHIFT':
                    final_v = a << b
                else:
                    #d1['connect_method'] == 'RSHIFT':
                    final_v = a >> b
                    
                dic2.update({name_str: final_v})

def main():
    f = open("2015-Q7_tyler.txt", 'r')
    lst = f.readlines()
    f.close()

    circuit = {}
    final_dic = {}

    for line in lst:
    
        if len(line.split()) == 3:
            element = {'connect_method': 'eql', 'value': line.split()[0]}
        elif len(line.split()) == 4:
            if line.split()[0] == 'NOT':
                element = {'connect_method': 'not', 'value': line.split()[1]}
            else:
                print('error')
                break
        elif len(line.split()) == 5:
            cm = line.split()[1]
            element = {'connect_method': line.split()[1], 'value': (line.split()[0], line.split()[2])}
        else:
            print('input error')
            break
    
        circuit.update({line.split()[-1]: element})

    import datetime
    t1 = datetime.datetime.now()

    while len(circuit) > len(final_dic):
        for line in circuit:
            if not_exist_newdic(line, final_dic):
                get_value(line, circuit, final_dic, 0)

    t2 = datetime.datetime.now()
    print(t2-t1)

if __name__ == '__main__': main()