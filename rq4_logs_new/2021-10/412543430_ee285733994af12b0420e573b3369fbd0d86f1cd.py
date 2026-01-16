def applyBitStuffing(s,a):
 x = a.replace (s,s+"0")
 return x
def applydeBitStuffing(s,x):
 y = x.replace (s+"0",s)
 return y
print("19012011002 Manthan Bhavsar")
a = input("enter the string: " )
s = "11111"
x = applyBitStuffing(s,a)
print("After Bit Stuffing: ",x)
y = applydeBitStuffing(s,x)
print("After Bit De-Stuffing: ",y)
#ex. 110101111100101111101010111110110