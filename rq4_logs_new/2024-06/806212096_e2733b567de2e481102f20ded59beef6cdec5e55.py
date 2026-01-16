class SimpleHashMap:
    def __init__(self, size=100):
        self.size = size
        self.buckets = [[] for _ in range(size)]

    def hash_function(self, key):
        return sum(int(ch) for ch in key if ch.isdigit()) % 10


    def put(self, key, value):
        index = self.hash_function(key)
        bucket = self.buckets[index]
        for i, (k,v) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, value)
                return 
        bucket.append((key,value))

    def get(self, key):
        index = self.hash_function(key)
        bucket = self.buckets[index]
        for i, (k, v) in enumerate(bucket):
            if k == key:
                return v

    def remove(self, key):
        index = self.hash_function(key)
        bucket = self.buckets[index]
        for i, (k,v) in enumerate(bucket):
            if k == key:
                del bucket[i]

    def __str__(self):
        st = ""
        for i, bucket in enumerate(self.buckets):
            st += f"Bucket-> {i}: {bucket}" + "\n"
        return st


hash_map = SimpleHashMap(size=10)

hash_map.put("123-4567", "Charlotte")
hash_map.put("123-4568", "Thomas")
hash_map.put("123-4569", "Jens")
hash_map.put("123-4570", "Peter")
hash_map.put("123-4571", "Lisa")
hash_map.put("123-4672", "Adele")
hash_map.put("123-4573", "Michaela")
hash_map.put("123-6574", "Bob")

print(hash_map)

print("\nName associated with '123-4570':", hash_map.get("123-4570"))

print("Updating the name for '123-4570' to 'James'")
hash_map.put("123-4570","James")

# Checking if Peter is still there
print("Name associated with '123-4570':", hash_map.get("123-4570"))