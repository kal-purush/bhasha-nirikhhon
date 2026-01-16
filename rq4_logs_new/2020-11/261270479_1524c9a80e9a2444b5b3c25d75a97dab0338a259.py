class UnionFind:
    
    def __init__(self):
        self.size = 0
        self.numComponents = 0
        self.szs = {}
        self.ids = {}

    def add(self, p):
        self.size += 1
        self.numComponents += 1
        self.szs[p] = 1
        self.ids[p] = p

    def contains(self, p):
        return self.ids.get(p, None) != None


    def find(self, p):
        r = p+0
        while r != self.ids[r]:
            r = self.ids[r]
        
        while p != r:
            pp = self.ids[p]
            self.ids[p] = r
            p = pp
        
        return r

    def connected(self, p, q):
        return self.find(p) == self.find(q)

    def componentsSize(self, p):
        return self.szs[self.find(p)]

    def size(self):
        return self.size

    def components(self):
        return self.numComponents

    def unify(self, p, q):
        root1 = self.find(p)
        root2 = self.find(q)
        if root1 == root2:
            return

        if self.szs[root1] < self.szs[root2]:
            self.szs[root2] = self.szs[root2] + self.szs[root1]
            self.ids[root1] = root2
        else:
            self.szs[root1] = self.szs[root1] + self.szs[root2]
            self.ids[root2] = root1
        
        self.numComponents -= 1
    
    def minmax(self):
        mini = self.size
        maxi = 0
        for i in self.ids:
            if i == self.ids[i]:
                mini = min(mini, self.szs[i])
                maxi = max(maxi, self.szs[i])
        
        return mini, maxi

    def roots(self):
        for i in self.ids:
            if i == self.ids[i]:
                yield i