int = 1 
float = 1.5 
array = [1,2,3] 
string = 'words' 
string2 = "words" 
array_of_char = ['a', 'b', 1] 
dict = { 'key': 'value' } 

boolean_t = True 
boolean_f = False 


print(int) 
print(float) 
print(string) 
print(string2) 
print(array_of_char) 
print(array) 
print(dict) 
print(boolean_t) 
print(boolean_f) 
print( array[1] ) 


print( dict['key'] ) 


def myfunction(value):  
  print 'the value is: ' +  str(value) 
  
  myfunction(array) 
  
def add_ints(number1, number2):  
    print number1 + number2 
    
    
add_ints(5,6) 
# this does nothing 


int = 1 


if int == 0:  
  print('hello') 
elif int == 1:  
  print('Hey') 
else:  print('hi')     


list = [1,2,3,4] 

for number in list:  
  if number % 2 == 0 and number % 4 == 0:    
    print number   
    
    
for char in string:  
  print char