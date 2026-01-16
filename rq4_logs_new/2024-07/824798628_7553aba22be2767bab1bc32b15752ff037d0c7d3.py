marks={"shubham":95,
       "shyam":85,
       "karan":"fail",
       "Rohan": True,
       "Rohit":65,
       "list":["Mango","Grapes",23,67,"Orange"],
       0:"suraj"
       }

print(marks.items())  #   ---> returns key value pairs in the form of tuples

print(marks.keys())  #--------> return keys in form of list


marks.update({"Rohan":False})  #----> this will update Rohan true to False
marks.update({"Shreya":98})      # ----> This will add shreya key value pair in last of dictionary
print(marks)

print(marks.get("Rohan"))  # prints the value of value given key
print(marks.get("Ram"))   # returns none if key is not present in dictionary
# print(marks["Ram"])         #generates error if key is not  present in Dictionary

print(marks.pop(0))     #----> removes the particular key value pair
print(marks)

value= marks.popitem() #-------> Removes the last key value pair fro last of dicionary
print(value)
print(marks)