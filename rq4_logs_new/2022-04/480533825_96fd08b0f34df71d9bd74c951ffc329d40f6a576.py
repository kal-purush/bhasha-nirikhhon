'''
HashTable Exercise 1
'''
import csv

class HashTable:
    def __init__(self):
        self.MAX = 100
        self.arr = [[] for i in range(self.MAX)]
    
    def get_hash(self, key):
        h = 0
        for char in key:
            h += ord(char)
        return h % self.MAX
    
    def __setitem__(self, key, val):
        h = self.get_hash(key)
        # self.arr[h] =val # overides index h, so it can't handle collisions yet
        found = False
        # istead create a linked list for when you need collision handling
        for idx, element in enumerate(self.arr[h]):
            # handling the case where the key exists already
            if len(element) == 2 and element[0] == key:
                self.arr[h][idx] = (key,val)
                found=True
                break
        # handles the case where the key has not yet existed
        if not found:
            self.arr[h].append((key, val))
    
    def __getitem__(self, key):
        h = self.get_hash(key)
        # return self.arr[h] # retrieves everything in the particular index
        for element in self.arr[h]:
            if element[0] == key:
                return element[1]
    
    def __delitem__(self, key):
        h = self.get_hash(key)
        for idx, element in enumerate(self.arr[h]):
            if element[0] == key:
                del self.arr[h][idx]

t = HashTable()
# reading csv file
with open('nyc_weather.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    sum = 0
    avg = 0
    max = 0
    # iterating through to get the 7 day's temperatures and take the average of it:
    for idx, t in enumerate(reader):
        if int(t['temperature(F)']) > max:
            max = int(t['temperature(F)'])
        if idx < 7:
            sum += int(t['temperature(F)'])
    avg = sum/7
    avg = int(avg)
    print("The average is: ", avg, " degrees F")
    print("The max: ", max)