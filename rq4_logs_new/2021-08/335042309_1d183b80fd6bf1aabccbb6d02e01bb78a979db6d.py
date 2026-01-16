class DSet:

    def __init__(self, n):
        self.parents = list(range(n))
        self.rank = [1]*n

    def find(self, idx): # Find parent of idx 

        if self.parents[idx] == idx:
            return idx 
        
        return self.find(self.parents[idx])

    def union(self, idx1, idx2):

        p1 = self.find(idx1)
        p2 = self.find(idx2)

        self.parents[p1] = p2 

    def find_compressed(self, idx):

        if self.parents[idx]==idx:
            return idx 

        self.parents[idx] = self.find_compressed(self.parents[idx])

        return self.parents[idx]

    def union_rank(self, idx1, idx2):

        p1 = self.find_compressed(idx1)
        p2 = self.find_compressed(idx2)

        if p1==p2:
            return 
        
        if self.rank[p1]>self.rank[p2]:
            self.parents[p2] = p1
        elif self.rank[p1]<self.rank[p2]:
            self.parents[p1] = p2 
        else:
            self.parents[p1] = p2 
            self.rank[p2] += 1

obj = DSet(5)
obj.union_rank(0, 2)
obj.union_rank(4, 2)
obj.union_rank(3, 1)
if obj.find_compressed(4) == obj.find_compressed(0):
    print('Yes')
else:
    print('No')
if obj.find_compressed(1) == obj.find_compressed(0):
    print('Yes')
else:
    print('No')