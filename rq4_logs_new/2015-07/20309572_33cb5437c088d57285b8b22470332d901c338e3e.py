#!/usr/bin/python
import sys

class ham:
 code_type = 8
 parity = 0
 def decode(self, data=0 ):
  if data: 
   data = str(data)
  else:
   data = str( self.data )
  temp = ""  
  for i in range( self.parity ):
   temp += str( int ( self.create_parity( data, 2**i ) ) % 2 )
  # print temp % 2
  #print temp % 2
  print int(temp[::-1], 2)
   
 def encode(self, data=0 ):
  self.enc_data = list();
  if data == 0:
   data = self.data
   for i, j in zip( data[0::2], data[1::2]):
    raw_str = i+j
    #add null for 2**i -bits
    for par in range(self.parity):
     raw_str = raw_str[0:2**par-1] + '0' + raw_str[2**par-1:]
     #codding string with null 
     #print(raw_str)
    for i in range(self.parity):
     #print i
     raw_str = raw_str[0:2**i-1] + self.create_parity( raw_str, 2**i ) \
      + raw_str[2**i:]
    print(raw_str)
   #return raw_str
    self.enc_data.append( raw_str) 
  else:
   data =  str( bin( int(data) ))[2:] 
   for i in range( 8 - len(data)): data = '0' + data
   #print data
   for par in range(self.parity):
    data = data[0:2**par-1] + '0' + data[2**par-1:]
   for i in range(self.parity):
    data = data[0:2**i-1] + self.create_parity( data, 2**i ) \
     + data[2**i:]
   #print data 
  return self.enc_data

 def create_parity( self, data, step):
  temp = ""
  if step == 1:
   temp = data[::step+1]
  else:
   count = step
   while count < len(data):
    temp += data[count-1:count+step-1]
    count+=step*2
  #print( temp, int(temp,2 ))
  count = 0
  for i in temp: 
   if i == '1':
    count +=1
  return str(count % 2)

  
 def __init__(self, arg=0, mode='digit'):
  self.data = list()
  if mode == 'symbol':
   for j in arg:
    for i in range(len(j)):
     self.data.append(bin( ord(arg[i])) )
   print(self.data)
  elif mode == 'digit':
   for j in arg:
    dig = bin(int(j))[2:]
    for i in range( 8 - len(dig)): dig = '0' + dig
    self.data.append( dig )
   print( self.data); 
  self.choose_code( self.data )
  #self.encode( self.data )
 def choose_code(self, arg ):
  if len(arg) % 2 == 0:
   self.code_type = 16
  self.parity_bits()
 def parity_bits( self ):
  self.parity = len(bin( self.code_type)[2:]) 

try:
  arg = sys.argv[1:]
except IndexError:
  arg = "total"
a = ham(arg);
a.encode()
#a = ham(arg)
#a.encode(arg[0])
a.decode("001001100000101101110")
a.decode("100110000110001011101")
a.decode( "100110000110001011101" )
a.decode( "001001100000101101110" )
a.decode( "001001100000101001110" )
a.decode( "100110000100001011101" )
#print a.enc_data