class Empty(Exception):
    pass

class ArrayStack:
    def __init__(self):
        self._data = []

    def len(self):
        return len(self._data)

    def is_empty(self):
        return len(self._data) == 0

    def push(self, e):
        self._data.append(e)

    def top(self):
        if self.is_empty():
            raise Empty("Stack is empty")
        return self._data[-1]

    def pop(self):
        if self.is_empty():
            raise Empty("Stack is empty")
        return self._data.pop()

def reverse_file(filename):
    S = ArrayStack()
    original = open(filename)
    for line in original:
        S.push(line.rstrip("\n"))
    original.close()

    output = open(filename, "w")
    while not S.is_empty():
        output.write(S.pop() + "\n")
    output.close()

    ofile = open(filename, "r")
    k = ofile.readlines()
    for i in k:
        print(i.rstrip())

def is_matched(expr):
    lefty = "({[" 
    righty = ")}]"
    S = ArrayStack()
    for c in expr:
        if c in lefty:
            S.push(c) 
        elif c in righty:
            if S.is_empty():
                return False 
            if righty.index(c) != lefty.index(S.pop()):
                return False
    return S.is_empty()

active = True

while active :
    print("\nMenu : \n 1. Reverse File \n 2. Matching Delimiters \n 3. Keluar")
    pilih = int(input("Input Pilihan Menu : "))
    if pilih == 1 :
        namaFile = input("Input Nama File : ")
        print(namaFile + ".txt")
        reverse_file(namaFile + ".txt")
    elif pilih == 2 :
        expression = input("Input Expression : ")
        match = is_matched(expression)
        if match :
            print("\nSemua delimiters cocok dengan baik")
        else :
            print("\nTerdapat delimiters yang tidak cocok")
    else :
        break