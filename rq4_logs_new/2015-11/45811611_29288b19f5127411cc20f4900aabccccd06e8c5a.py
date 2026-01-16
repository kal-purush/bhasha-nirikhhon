def fun(a,b,c):
   if a == b and b == c:
      return 1
   elif a != b and b !=c and a != c:
      return 3
   else:
      return 2


def main0():
  print(fun(1,2,3))
  print(fun('sam','sam','sam'))
  print(fun('sam','is','sam'))
main0()


def fun1(lst):
   ''' lst is a list of non-negative ints.'''
   x = -1
   for i in lst:
      if i > x:
        x = i
   return x

def main1():
   print(fun1([4,234,14]))
   print(fun1([]))
main1()




def fun2(L):
    '''L is a list of lists. '''
    lens = []
    for l in L:
        lens.append(len(l))
    return lens

def main2():
   print(fun2([[4,5,6],["hi"],[2.2,3.3,4.4,5.5]]))
main2()



def fun3(s1,s2):
   '''s1 and s2 are strings.'''
   if len(s1) > len(s2):
      return False
   for i in range(len(s1)):
      if not s1[i] == s2[i]:
         return False
   return True


def main3():
   print(fun3('eee','e'))
   print(fun3('e','eee'))
   print(fun3('help','help me'))
   print(fun3('comp','science'))
   print(fun3('',''))
main3()




def fun4(s,c):
   '''s is a string and c is a char.'''

   returnVal = -1
   for i in range(len(s)):
      if s[i] == c:
         return i
   return returnVal

def main4():
   print(fun4("i know the voices in my head aren't real",'o'))
   print(fun4("zzzzzzzzz","o"))
main4()