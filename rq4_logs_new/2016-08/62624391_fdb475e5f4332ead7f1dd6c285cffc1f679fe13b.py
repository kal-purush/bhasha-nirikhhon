"""
module demonstrates the methods in a dictionary. The values retuned by these
methods are not true lists and they cannot be modified. But these can be used
in for loops.
"""
"""
BLOCK - 1
"""

dict1 = {'name' : 'Raghu', 'age' : '21'}

for detail1 in dict1.values():
    print(detail1)
"""
OUTPUT :
21
Raghu
"""

for detail2 in dict1.keys():
    print(detail2)

"""
OUTPUT :
age
name
"""

for detail3 in dict1.items():
    print(detail3)

"""
OUTPUT :
('age', '21')
('name', 'Raghu')
The output of the above operation is a tuple and  the same can be converted
to a list as explained below.
"""
for detail3 in dict1.items():
    print(list(detail3))

"""
OUTPUT :
['age', '21']
['name', 'Raghu']
"""

"""
Validation of keys and Values in a dictionary.
If present, returns True if not returns False.
"""

dict2 = {'name' : 'Raghu', 'age' : '21'}

print('name' in dict2.keys()) #OUTPUT : TRUE
print('21' in dict2.values()) #OUTPUT : TRUE
print('age' in dict2.values()) #OUTPUT : FALSE

"""
In case of absence of keys in a dictionary, compiler throws an error,
saying "KeyError". Byt get() functions helps to avoid those scenarios.
It takes 2 aruguments, a key value to return and other is fallback value
in case of absence of the the "Key"
"""

dict3 = {'name' : 'Raghu', 'age' : '21'}

print("Hello " + dict3.get('name', 0))
print("I am from " + dict3.get('place', 'INDIA'))

"""
If the above operation is executed without get() function, it results in
KeyError. Try this.
print("I am from " + dict3['place'])
"""

"""
Any new key can be added to a dictionary in case it is not available.
setdefault() fucntions helps in doing so.
"""

dict4 = {'name' : 'Raghu', 'age' : '21'}
dict4.setdefault('place', 'INDIA')
print(dict4)

"""
OUTPUT :
{'age': '21', 'name': 'Raghu', 'place': 'INDIA'}
If we try to add new Value to the existing key , the new Value will not be
considered. Try this.

dict4.setdefault('place', 'BHARAT')
print(dict4)

OUTPUT : {'place': 'INDIA', 'age': '21', 'name': 'Raghu'}
NOTE : Items in a dictionary are not stored in an order, so everytime you try
to access the items, they are displayed in different order each time.
"""
























































