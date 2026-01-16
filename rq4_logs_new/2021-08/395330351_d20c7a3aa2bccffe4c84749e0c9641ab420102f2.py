# dictionary: key-value pair, unordered, mutable

mydict = {'name':'Max','age':20,'city':'New York'} #initializing a dictionary
print(mydict)

mydict2 = dict(name="Tareq",age=20,city="Dhaka")  #initializing a dictionary
print(mydict2)
value = mydict['city']
print(value)

mydict['email'] = 'mtareq@gmail.com'
value1 = mydict['email']
print(value1)
# del mydict['city'] # deleting a key from the dictionary
# mydict.pop('age') # deleting a key from the dictionary
#mydict.popitem() # deleting the last item from the dictionary
print(mydict)

if 'name' in mydict: # checking if a key value pair exists or not
    print (mydict['name'])
else:
    print("name doesnt exist")

try:                    # checking if a key value pair exists or not
    print(mydict['university'])
except:
    print("Error, not found!")

for key,value in mydict.items(): # iterating through a dictionary and printing key value pairs
    print(key,value)

mydict_cpy = mydict # copy but also modifies the original mydict if it is modified
mydict_cpy_act = mydict.copy() # actual copy
mydict_cpy_act_1 = dict(mydict) # actual copy

x = {'name':'Tom','age':23, 'city':'Las Vegas'}
y = {'name':'Jack','age':25,'email':'jake@gmail.com'}

x.update(y) #  update the dictionary by overriding and then adding any new fields
print(x)
#keytype can be numbers, string, tuples