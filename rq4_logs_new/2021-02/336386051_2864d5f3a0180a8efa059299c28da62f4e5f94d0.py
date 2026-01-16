def expression_1(x):
    import math
    var1= math.pow(x,3)
    var2= math.pow(x,2)
    print(var1 - (2*x+var2)-100)
expression_1(1)

def expression_2(x, y):
    import math
    var1= math.pow(x,4)
    var2= 2*y
    var3= 3*x*y
    var4=6*y
    var5=7*x
    print (int((var1/var2)-var3+(var4/var5)))
expression_2(1,2)

def expression_3(x, y):
    import math
    var1 = math.pow(x,3)
    var2 = math.pow(y,2)
    var3 = math.pow(x,2)
    var4 = math.pow(y,3)
    print((var1+var2)/(var3-var4))
expression_3(1,2)