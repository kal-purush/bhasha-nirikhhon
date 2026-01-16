import modulo
import ElliptischeKromme as EK
import random

def find_factor(N, max_iter = 10, max_factorial = 10): #zoek redelijke standaardwaarden
    factor = None
    
    """def eval_gcd(v, N, n):
        nonlocal factor
        if v not in [1, N]:
            factor = n"""

    
    class MonitorMod(modulo.IntModP(N)):
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
                    
            v, x, _ = eea(self.p, self.n)
            #eval_gcd(v, N, self.n)
            nonlocal factor
            if v not in [1, N]:
                factor = v
            return MonitorMod(x)
            
    class MonitorKromme(EK.ElliptischeKromme):
        def testPunt(self, x, y):
            if factor:
                return True
            else:
                return super().testPunt(x, y)


    
    def search_factor():
        x = MonitorMod(random.randint(0, N-1))
        y = MonitorMod(random.randint(0, N-1))
        a = MonitorMod(random.randint(0, N-1))
        b = y*y - x*x*x - a*x
        C = MonitorKromme(a, b)
        P = EK.Punt(x, y, C)
        for i in range(1, max_factorial + 1):
            P = i * P
            if factor:
                return factor
            elif P == EK.NulPunt(C):
                return None
        return None
        
    for j in range(max_iter):
        ans = search_factor()
        if ans:
            return ans
            
    return None