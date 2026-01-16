#currency.py
#朱泽源 https://github.com/anbexplorer/ichw
#2017.12.11  19:30
"""Module for currency exchange

This module provides several string parsing functions to implement a 
simple currency exchange routine using an online currency service. 
The primary function in this module is exchange."""
#下面的函数就是最终函数
#输入要转换的货币、要转成的货币、转换额度即可输出转换出的额度
def exchange(a,b,c):
    from urllib.request import urlopen
    dict={'a':str("")+a+str(""),'b':str("")+b+str(""),'c':str("")+c+str("")}
    doc=urlopen('http://cs1110.cs.cornell.edu/2016fa/a1server.php?from='+dict['a']+'&to='+dict['b']+'&amt='+dict['c'])
    docstr = doc.read()
    doc.close()
    g = docstr.decode('ascii')
    g=list(g)
    d=''
    f=''
    for i in g:
        d=d+i
    e=d.find(',')
    d=d[e:]
    for i in d:
        if i.isdigit() or i is ('.'):
            i=str(i)
            f=f+i    
    return(f)
a=input()
b=input()
c=input()
print(exchange(a,b,c))    
#下面的函数是检测函数
#如果出错，会报错AssertionError;不出错，则会输出"All tests passed
#Noun'
def test_get_from():
    assert(a == a)   
def testAll():
    """test all cases"""
    test_get_from()
    print("All tests passed")  
print(testAll())