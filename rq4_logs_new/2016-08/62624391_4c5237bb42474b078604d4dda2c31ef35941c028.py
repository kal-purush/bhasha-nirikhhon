"""
module demonstrates the right way to copy structures like lists and dictionaries.
"""

list1 = [1,2,3,4,5]
list2 = list1

list2.append(10)

"""
when a list is assigned to a new list, both the lists refer to same memory
location.The following print results as:
LIST1 :  [1, 2, 3, 4, 5, 10] 
LIST2 :  [1, 2, 3, 4, 5, 10]
"""
print("LIST1 : ",list1, "\nLIST2 : ", list2)

"""
So, copying of lists can be done in the following manner.
"""

list3 = list(list1)

list3.append(20)
print("LIST1 : ",list1, "\nLIST3 : ", list3)

"""
Output of above operations:
LIST1 :  [1, 2, 3, 4, 5, 10] 
LIST3 :  [1, 2, 3, 4, 5, 10, 20]
"""

dict1 = {'x' : {'x1' : 10, 'x2' : 20}, 'y' : {'y1' : 30, 'y2' : 40}}
dict2 = dict(dict1)

dict2['y']['y2'] = 50

print("DICT1 : ",dict1, "\nDICT2 : ", dict2)

"""
OUTPUT:
DICT1 :  {'x': {'x2': 20, 'x1': 10}, 'y': {'y1': 30, 'y2': 50}} 
DICT2 :  {'x': {'x2': 20, 'x1': 10}, 'y': {'y1': 30, 'y2': 50}}

similar kind of dict() operation to copy a dict can result as above.
"""
from copy import deepcopy, copy

dict1 = {'x' : {'x1' : 10, 'x2' : 20}, 'y' : {'y1' : 30, 'y2' : 40}}
dict3 = deepcopy(dict1)

dict3['y']['y2'] = 100

print("DEEPCOPY RESULTS")
print("DICT1 : ",dict1, "\nDICT3 : ", dict3)

"""
OUTPUT:
DICT1 :  {'y': {'y1': 30, 'y2': 40}, 'x': {'x2': 20, 'x1': 10}} 
DICT3 :  {'y': {'y1': 30, 'y2': 100}, 'x': {'x2': 20, 'x1': 10}}
copying of dictonaries can be done using deepcopy() function whereas copy()
fucntion cannot be used for dictionaries and can be used only for lists
"""
dict1 = {'x' : {'x1' : 10, 'x2' : 20}, 'y' : {'y1' : 30, 'y2' : 40}}

dict4 = copy(dict1)
dict4['y']['y2'] = 400

print("DICT1 : ",dict1, "\nDICT4 : ", dict4)

"""
the copy() function results as follows which is similar to the dict()
functionality.
OUTPUT:
DICT1 :  {'y': {'y1': 30, 'y2': 400}, 'x': {'x2': 20, 'x1': 10}} 
DICT4 :  {'y': {'y1': 30, 'y2': 400}, 'x': {'x2': 20, 'x1': 10}}
"""