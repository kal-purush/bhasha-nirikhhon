import numpy as np

class GE():
    mtx_U = []
    mtx_b = []
    mtx_P = []
    mtx_x = []

    def __init__(self, A, b):
        l = len(A)
        P = np.identity(l)
        for i in range(0, l):
            for j in range(l-1, i, -1):
                constant = float(A[j][i]/A[i][i])
                for k in range(i, l):
                    A[j][k] = float(A[j][k] - constant * A[i][k])
                for k in range(0, l):
                    P[j][k] = float(P[j][k] - constant * P[i][k])
                b[j][0] = float(b[j][0] - constant * b[i][0])
        x = [[0]]*l
        for i in range(l-1, -1, -1):
            sum = b[i][0]
            for j in range(i+1, l):
                sum -= float(A[i][j] * x[j][0])
            if A[i][i] == 0:
                x[i] = [x]
            else:
                x[i] = [float(sum / A[i][i])]
        self.mtx_U = A
        self.mtx_P = P
        self.mtx_b = b
        self.mtx_x = x

    def get_U(self):
        return self.mtx_U

    def get_P(self):
        return self.mtx_P

    def get_sol(self):
        return self.mtx_x
