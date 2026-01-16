#!/usr/bin/env python
# coding: utf-8

# In[ ]:


size=int(input())
# your code goes here
import textwrap
s='abcdefghijklmnopqrstuvwxyz'
L=textwrap.wrap(s,size)
string=str(L[0])
lisr=textwrap.wrap(string,1)
lisr.reverse()
reversed_string=''.join(lisr)
fstring=reversed_string[0]
leng=1+4*(size-1)
m=size-1
for i in range(2,size+2):
        
    print(fstring.center(leng,'-'))
    sfstring=reversed_string[:i]+string[-(i-1):]
    l=textwrap.wrap(sfstring,1)
    fstring='-'.join(l)    
        
         
for j in range(1,size-1):
    sfstring=reversed_string[:m]+string[-(m-1):]
    l=textwrap.wrap(sfstring,1)
    fstring='-'.join(l)        
    print(fstring.center(leng,'-'))
    m-=1
if(size!=1):    
    fstring=reversed_string[0]
    print(fstring.center(leng,'-'))
