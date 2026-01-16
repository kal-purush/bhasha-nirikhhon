
HEX_TABLE = {'0': 0, '1': 1, '2': 2, '3': 3,
            '4': 4, '5': 5, '6': 6, '7': 7,
            '8': 8, '9': 9, 'A': 10, 'B': 11,
            'C': 12, 'D': 13, 'E': 14, 'F': 15}

conversion_table = {0: '0', 1: '1', 2: '2', 3: '3', 4: '4',
                    5: '5', 6: '6', 7: '7',
                    8: '8', 9: '9', 10: 'A', 11: 'B', 12: 'C',
                    13: 'D', 14: 'E', 15: 'F'}

def bin_dec(n):
    n = str(n)
    d = 0
    for i in range(len(n)-1):
        print(n[i]+"x2^"+str(len(n)-i-1)+ " + ", end="")
        d += int(n[i])*(2**(len(n)-i-1))
    print(n[-1]+"x2^"+str(0)+ " = ", end="")
    d += int(n[-1])*(2**(0))
    print(d, end="")

def dec_bin(n):
    if n >= 1:
        print(n, "/", 2, "=>", "cociente =", n//2, "\t resto =", n%2)
        dec_bin(n // 2)
    
    print(n % 2, end = '')

def oct_dec(n):
    n = str(n)
    d = 0
    for i in range(len(n)-1):
        print(n[i]+"x8^"+str(len(n)-i-1)+ " + ", end="")
        d += int(n[i])*(8**(len(n)-i-1))
        
    print(n[-1]+"x8^"+str(0)+ " = ", end="")
    d += int(n[-1])*(8**(0))
    print(d, end="")

def dec_oct(n): 
   octal = 0
   count = 1
   deci = n
 
   while (deci != 0):
        print(deci, "/", 8, "=>", "cociente =", deci//8, "\t resto(octal) =", oct(deci%8))
        remainder = deci % 8
        octal += remainder * count
        count = count * 10
        deci = deci // 8
 
   print(octal)

def hex_dec(n):
    res = 0
    size = len(n) - 1
    
    for num in n[:-1]:
        print(str(HEX_TABLE[num])+"x16^"+str(size)+ " + ", end="")
        res = res + HEX_TABLE[num]*16**size
        size = size - 1

    print(str(HEX_TABLE[n[-1]])+"x16^"+str(0)+ " = ", end="")
    res = res + HEX_TABLE[n[-1]]*16**0

    print(res)

def dec_hex(n): 
    hexadecimal = ''
    while(n > 0):
        remainder = n % 16
        hexadecimal = conversion_table[remainder] + hexadecimal
        print(n, "/", 16, "=>", "cociente =", n//16, "\t resto(hex) =", conversion_table[remainder])
        n = n // 16
 
    print(hexadecimal)
