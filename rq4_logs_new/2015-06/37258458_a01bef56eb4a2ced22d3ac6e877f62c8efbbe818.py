def IntModP(p):
    class IntegerModP(object):
        def __init__(self, n):
            self.n = n % p
        
        def __int__(self):
            return int(self.n)
        
        def __add__(self, other):
            return IntegerModP(int(self) + int(other))
        
        def __radd__(self, other): return self + other
        
        def __sub__(self, other):
            return IntegerModP(int(self) - int(other))
            
        def __rsub__(self, other): return other + (-self)
            
        def __mul__(self, other):
            return IntegerModP(int(self) * int(other))
        
        def __rmul__(self, other): return self * other
        
        def __div__(self, other):
            return self * other.inverse()
            
        def __truediv__(self, other):
            return self * other.inverse()
        
        def __neg__(self):
            return IntegerModP(-self.n)
            
        def __eq__(self, other):
            return isinstance(other, IntegerModP) and self.n == other.n
            
        def __divmod__(self, other):
            q, r = divmod(self.n, other.n)
            return (IntegerModP(q), IntegerModP(r))
        
        def __repr__(self):
            return '{} (mod {})'.format(self.n, self.p)
            
        def inverse(self):
            def eea(r0, r1, s0 = 1, s1 = 0, t0 = 0, t1 = 1):
                q, r = divmod(r0, r1)
                r0 = r1
                r1 = r
                s = s0 - q*s1
                s0 = s1
                s1 = s
                t = t0 - q*t1
                t0 = t1
                t1 = t
                if r1 == 0:
                    return r0, t0, s0
                else:
                    return eea(r0, r1, s0, s1, t0, t1)
                    
            _, x, _ = eea(self.p, self.n)
            return IntegerModP(x)
            
            
    IntegerModP.p = p
    IntegerModP.__name__ = 'Z/{}'.format(p)
    return IntegerModP 
        