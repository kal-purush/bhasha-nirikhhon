from __future__ import division

class matrix:

        def __init__(self,values):
                self.values = values
                print "NEW", values
                self.N = len(values)
                self.M = len(values[0])

        def display(self):
                print self.values

        def row(self, n):
                return self.values[n-1]

        def column(self, m):
                c = []
                for r in self.values:
                        c.append(r[m-1])
                return c

        def operate(self, v):
                newmat = []
                for n in range(1,self.N+1):
                        newrow = []
                        for m in range(1,v.M+1):
                                vr = v.row(n)
                                selfc = self.column(m)
                                toa = sum(p*q for p,q in zip(vr, selfc))
                                newrow.append(toa)
                                print toa
                        newmat.append(newrow)
                nmatobj = matrix(newmat)
                print nmatobj.values
                return nmatobj


m = matrix([[1,2],[3,4]])
a = matrix([[0,1],[1,0]])
a.operate(m)