class SimpleHashSet:
    def __init__(self, size=100):
        self.size = size
        self.buckets = [[] for _ in range(size)]

    
    def hash_function(self, value):
        return sum(ord(ch) for ch in value) % self.size

    
    def add(self, value):
        index = self.hash_function(value)
        bucket = self.buckets[index]
        
        if value not in bucket:
            bucket.append(value)


    def contains(self, value):
        index = self.hash_function(value)
        bucket = self.buckets[index]
        return value in bucket

    def remove(self, value):
        index = self.hash_function(value)
        bucket = self.buckets[index]
        if value in bucket:
            bucket.remove(value)
            
        
    def __str__(self):
        st = ""
        for index, bucket in enumerate(self.buckets):
            st += str({f"Bucket : {index}, {bucket}"}) + " \n"

        return st
    

hash_set = SimpleHashSet(size=10)

hash_set.add("Charlotte")
hash_set.add("Thomas")
hash_set.add("Jens")
hash_set.add("Peter")
hash_set.add("Lisa")
hash_set.add("Adele")
hash_set.add("Michaela")
hash_set.add("Bob")

print(hash_set)


print("\n'Peter' is in the set:",hash_set.contains('Peter'))
print("Removing 'Peter'")
hash_set.remove('Peter')
print("'Peter' is in the set:",hash_set.contains('Peter'))
print("'Adele' has hash code:",hash_set.hash_function('Adele'))