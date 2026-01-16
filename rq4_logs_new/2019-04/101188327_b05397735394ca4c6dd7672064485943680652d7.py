import sys

cases = int(sys.stdin.readline())
m = sys.stdin.readline()
while cases > 0:
    m = int(m)
    x1 = bin(m)
    b1 = sum(int(x) for x in str(x1) if x == '1')
    x2 = bin(int('0x'+str(m),16))[2:]
    b2 = sum(int(x) for x in str(x2) if x == '1')
    #res = bool(m) ^ bool(b1*b2)
    print(str(b1) + " " + str(b2))
    cases -= 1
    m = sys.stdin.readline()