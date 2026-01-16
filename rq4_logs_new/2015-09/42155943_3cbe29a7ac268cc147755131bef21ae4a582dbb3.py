#!/usr/bin/env python3
'''
========================================================|
-------------------------------~~~~~~~~~~~~~~~~~~~~~~~~~|
Code Written By: Zackery Wilson~~~~~~~~~~~~~~~~~~~~~~~~~|
-------------------------------~~~~~~~~~~~~~~~~~~~~~~~~~|
This class is designed to generate sets of numbers.  I~~|
am adding functionality to print each set as cleanly~~~~|
as possible.~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|
========================================================| 
'''
class Set_Manager(object):
    def print_set(self):
        import pprint
        pprint.pprint(self.set)    
    def write_file(self):
        import pprint
        log = open(self.filename, 'w')
        pprint.pprint(self.set, log)
        log.close()
    def print_dict(self):      
        l = []                
        for key in self.set: 
            for value in self.set[key]:
                l.append(value)
        column_width = (len(str(max(l))) + 1)*2  #this is getting the length of the longest number in the dictionary
        l = []                                   #this is just to remove excess memory usage
        key_digits = 0
        value_digits = 0
        for key in self.set:
            key_digits = len(str(key))-1           
            print(str(key) + ':', end='')
            for value in self.set[key]:
                print('{0}{1}'.format(' ' * (column_width-(key_digits + value_digits)), value), end='')
                value_digits = len(str(value))-1 
                key_digits = 0
            print(' ')
            value_digits = 0
    def write_dict(self):
        log = open(self.filename, 'a')
        l = []
        for key in self.set:
            for value in self.set[key]:
                l.append(value)
        column_width = (len(str(max(l))) + 1) * 2
        l = []                                        #just to remove excess memory usage
        key_digits = 0
        value_digits = 0
        for key in self.set:
            key_digits = len(str(key)) - 1            #setting the amount of digits-1 of the current key to 'x'.       
            print(str(key) + ':', file=log, end='')
            for value in self.set[key]:                     
                print('{0}{1}'.format(' ' * (column_width-(key_digits + value_digits)), value), file=log, end='')
                value_digits = len(str(value))-1     #setting the amount of digits-1 of the current value to 'y'.                     
                key_digits = 0                       #setting x back to 0 before reiterating.
            print(' ', file=log)
            value_digits = 0
        log.close()

class Multiples(Set_Manager):
    def __init__(self, mult_limit, limit):
        self.mult_limit = mult_limit
        self.limit = limit
        self.set = { key: list(range(key, ((key * self.mult_limit) + 1), key)) for key in range(1, (self.limit + 1)) }  
    def print_set(self):                                    
        Set_Manager.print_dict(self)
    def write_dict(self, filename):
        self.filename = filename
        Set_Manager.write_dict(self)
    def write_file(self, filename):
        self.filename = filename
        Set_Manager.write_dict(self)

class Fibonacci(Set_Manager):
    def __init__(self, limit_amount):
        self.limit = limit_amount + 1
        self.set = []
        a = 0
        b = 1
        for i in range(1,self.limit):
            c = a + b
            self.set.append(c)
            a = b
            b = c
    def print_set(self):
        Set_Manager.print_set(self)
    def print_dict(self):
        print('Fibonacci does not support this. Please use "print_set()"')
    def write_dict(self, filename):
        print('Fibonacci does not support this. Please use "write_file()"')
    def write_file(self, filename):
        self.filename = filename
        Set_Manager.write_file(self)

class Squares(Set_Manager):
    def __init__(self, limit_amount=2, limit_set=5):
        self.set_limit = limit_set+1
        self.limit = limit_amount + 1
        self.set = { i: [ i**x for x in range(1,self.set_limit) ] for i in range(2, self.limit) }
    def print_single(self, key):
        l = list(self.set[key])
        print(l)
    def print_set(self):
        Set_Manager.print_dict(self)
    def write_dict(self, filename):
        self.filename = filename
        Set_Manager.write_dict(self)
    def write_file(self, filename):
        self.filename = filename
        Set_Manager.write_dict(self)
        

class ascii(Set_Manager):
    def __init__(self):
        self.set = {i: chr(i) for i in range(1,128)}
    def print_set(self):
        Set_Manager.print_set(self)
    def print_dict(self):
        print('ascii does not support this.  Use "print_set()" instead.')
    def write_dict(self, filename):
        print('ascii does not support this Use "write_file()" instead')
    def write_file(self, filename):
        print('As of right now, not all ASCII characters appear properly in a text file.')

class Combos(Set_Manager):  #this whole class is terribly inneficient fix this shit. 
    def __init__(self, limit_amount):
        import itertools
        self.limit = limit_amount+1
        self.set = {}
        self.set2 = {}
        for h in range(1,self.limit):
            combos = 0
            l = [x for x in range(1,(h+1))]
            value = []
            for i in range(1, len(l)+1):
                for j in itertools.combinations(l, i):
                    combos += 1
                    value.append(j)
            self.set[i] = combos
            self.set2[i] = value
    def find_pattern(self):
        length = len(self.set)
        l = []
        for i in range(1,length):
            l.append(self.set[i+1]-self.set[i])
        print(l)
    def print_combos(self):            #combo specific method. try to inherit
        import pprint
        pprint.pprint(self.set2)
    def write_combos(self, filename):  #combo specific method. try to inherit
        self.filename = filename
        import pprint
        log = open(self.filename, 'w')
        pprint.pprint(self.set2, log)
        log.close()
    def print_set(self):
        Set_Manager.print_set(self)
    def print_dict(self):    #this will not work because my current function if specifically for dictionaries of lists. fix this
        Set_Manager.print_set(self)
    def write_dict(self, filename):
        self.filename = filename
        Set_Manager.write_file(self)
    def write_file(self, filename):
        self.filename = filename
        Set_Manager.write_file(self)