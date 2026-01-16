# A "dictionary" is a map from keys to values
d = {"key1" : 5, "key2" : "Hello", 7 : 100}
print d["key1"]
print d["key2"]
print d[7]
#print d["key3"]

emp = {"name": "Neal", "salary": "10000", "age": 93}
print emp

# Iterate through the keys of the dictionary
for i in emp:
    print emp[i]
