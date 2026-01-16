'''
Practicing implementing hashmaps
'''
# def get_hash(key):
#     h = 0
#     for char in key:
#         h += ord(char)
#     return h % 100

# out = get_hash('march 22')
# print(out)

class HashTable:
    def __init__(self):
        self.MAX = 100
        self.arr = [None for i in range(self.MAX)]

    # Hash function
    def get_hash(self, key):
        h = 0
        for char in key:
            h += ord(char)
        return h % self.MAX
    
    # add/insert function
    def __setitem__(self, key, val):
        h = self.get_hash(key)
        self.arr[h] =val
    
    # get function. Calling it by key
    def __getitem__(self, key):
        h = self.get_hash(key)
        return self.arr[h]
    
    #delete item function
    def __delitem__(self, key):
        h = self.get_hash(key)
        self.arr[h] = None

           

t = HashTable()
# using function
# print(t.__setitem__('march 6', 130)) 
# print(t.__getitem__('march 6'))

# using dictionary
t['march 6'] = 130 # setitem
t['march 1'] = 20
t['dec 17'] = 27

print(t['march 6']) # getitem
print(t['march 1'])
print(t['dec 17'])

del t['march 6'] # should delete 130 from the dictionary

print(t.arr) # printing dictionary
