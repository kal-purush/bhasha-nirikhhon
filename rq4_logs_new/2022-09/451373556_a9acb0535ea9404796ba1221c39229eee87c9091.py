def add_binary(a,b):
    #your code here
    decNum=[a+b]
    strings = [str(dec) for dec in decNum]
    an_bin = "". join(strings)
    a_string = int(an_bin)
    binNum=0
    power=0
    while a_string>0:
        binNum+=10**power*(a_string%2)
        a_string//=2
        power+=1
    return str(binNum)
print(add_binary(51,12))
print(add_binary(1,2))
print(add_binary(1,10))