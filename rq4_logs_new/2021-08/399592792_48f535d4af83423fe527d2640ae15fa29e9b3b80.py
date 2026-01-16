class Solution:
    # @param A : list of integers
    # @param B : integer
    # @return an integer
    def diffPossible(self, A, B):
        d = {}
        for i in A:
            if d.get(i) != None:
                d[i] = d[i]+1
            else:
                d[i] = 1
        for i in A:
            if B == 0:
                if d.get(B+i) > 1:
                    return 1
            else:
                if d.get(B+i) != None:
                    return 1
        return 0


A = [0, 1, 9, 10, 13, 17, 17, 17, 23, 25, 29, 30, 37, 38, 39, 39, 40,
     41, 42, 60, 64, 70, 70, 70, 72, 75, 85, 85, 90, 91, 91, 93, 95]
B = 83
s = Solution()
print(s.solve(A, B))