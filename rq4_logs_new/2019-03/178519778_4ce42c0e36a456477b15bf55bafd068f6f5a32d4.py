class convert:
    def __init__(self):
        self.nb = 0
        self.nd = 0

    def d2b(self,n):
        if n == 0:
            return
        r = n%2
        self.d2b(n//2)
        nb = self.nb*10 + r

    def b2d(self, n, p):
        if n == 0:
            return
        r = n%10
        self.nd += ((2**p) * r)
        return self.b2d(n//10, p+1)


    def decimal2binary(self):
        pass

