import nmw


class hb:
    def __init__(self, g, m, d):
        self.g = g
        self.d = d
        self.m = m
        self.ww = []
        self.zz = []

    def compare(self, x, y):  # Compares sequence elements x and y to determine the match/difference score
        if x == y:
            return self.m
        else:
            return self.d

    def ComputeAlignmentScore(self, x, y):
        L = [j * self.g for j in range(len(y) + 1)]
        K = [0 for _ in range(len(y)+1)]
        for i in range(1, len(x) + 1):
            L, K = K, L
            L[0] = i * self.g
            for j in range(1, len(y) + 1):
                L[j] = max(L[j - 1] + self.g, K[j] + self.g, K[j - 1] + self.compare(x[i - 1], y[j - 1]))
        return L

    def Hirschberg(self, x, y):
        if len(x) == 0:
            WW = '-' * len(y)
            ZZ = y
            print("1", WW, ZZ)
        elif len(y) == 0:
            WW = x
            ZZ = '-' * len(x)
            print("2", WW, ZZ)
        elif len(x) == 1 or len(y) == 1:
            align_nmw = nmw.nmw(self.g, self.m, self.d)
            f = align_nmw.F(x, y)
            z = ""
            w = ""
            WW, ZZ = align_nmw.EnumerateAlignments(x, y, f, w, z)
            WW = ''.join(WW)
            ZZ = ''.join(ZZ)
            print("3", WW, ZZ)
        else:
            print("here")
            i = len(x) // 2
            Sl = self.ComputeAlignmentScore(x[0:i], y)
            Sr = self.ComputeAlignmentScore(x[i:len(x)][::-1], y[::-1])
            S = [k + t for k, t in zip(Sl, Sr[::-1])]
            j = S.index(max(S))
            print(j)
            WW = ""
            ZZ = ""
            #for j in range(J):
            WWl, ZZl = self.Hirschberg(x[0:i], y[0:j])
            WWr, ZZr = self.Hirschberg(x[i:len(x)], y[j:len(y)])
            print("wwl", WWl, "wwr", WWr)
            print("zzl", ZZl, "zzr", ZZr)
            WW = WWl + WWr
            ZZ = ZZl + ZZr
        return WW, ZZ