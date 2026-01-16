import numpy as np



class Jacoby:

    def __init__(self, matrix, error):

        self.matrix = matrix
        self.dim = self.matrix.shape[0]
        self.p = 0  # first row off diagonal
        self.q = 1  # first column off diagonal
        self.calcs = self.dim*(self.dim-1)*0.5  # this is the total number of off-diagonal elements
        self.error = error

    def change_off_diagonals(self):

        if not(self.q < self.dim-1):
            # if we are at the end of a row
            self.p += 1
            self.q = self.p + 1

            if self.q == self.dim:
                # reinitialize
                self.p = 0
                self.q = 1
        else:
            self.q += 1

    def calculate_theta(self):
        return (self.matrix[self.q][self.q] - self.matrix[self.p][self.p])/(2.0*self.matrix[self.p][self.q])

    def calculate_tan(self, theta):
        return np.sign(theta)/(np.abs(theta) + np.power(theta**2+1.0, 0.5))

    def calculate_rotation_stuff(self):
        theta = self.calculate_theta()

        return self.calculate_tan(theta=theta)

    def calculate_tau_sin(self, tan):
        tan_x_half = tan / (1.0 + np.power(1.0 + tan ** 2, 0.5))
        sin = 2.0*tan_x_half/(1+tan_x_half**2)
        cos = (1.0-tan_x_half**2)/(1.0+tan_x_half**2)

        return tan_x_half * sin/(1.0+cos), sin

    def compute_rest_elements(self, p, q, sin, tau, set_index):
        k = 0

        other_index = q if set_index == p else p
        sign = -1.0 if set_index == p else 1.0

        while k < self.dim:
            if k == q or k == p:
                k += 1
                continue

            self.matrix[set_index][k] += sign * sin * (self.matrix[k][other_index] - sign * tau * self.matrix[k][set_index])
            self.matrix[k][set_index] = self.matrix[set_index][k]

            k += 1

    def compute_deviation(self):
        k = 0
        l = 0
        S = 0.0
        while k <= self.dim - 1:
            while l <= self.dim - 1:
                if l == k:
                    l += 1
                    continue
                S += self.matrix[k][l]**2
                l += 1
            k += 1

        return S

    def calculate(self):

        tan = 0
        s = 1000.0

        while s > self.error:

            p = self.p
            q = self.q
            # find theta

            if self.matrix[self.p][self.q] == 0.0:
                self.change_off_diagonals()
                continue

            tan = self.calculate_rotation_stuff()
            tau, sin = self.calculate_tau_sin(tan)
            print(tan, sin)
            self.matrix[p][p] -= tan * self.matrix[p][q]
            self.matrix[q][q] += tan * self.matrix[p][q]
            self.matrix[p][q] = 0.0
            self.matrix[q][p] = 0.0

            # compute A_k_p
            self.compute_rest_elements(p=p, q=q, sin=sin, tau=tau, set_index=p)
            # compute A_k_p
            self.compute_rest_elements(p=p, q=q, sin=sin, tau=tau, set_index=q)

            s = self.compute_deviation()


            self.change_off_diagonals()


test_matrix = np.array([[2.0, 1.0],[1.0, 2.0]])

thre_b_thre = np.array([[3.0,0.0,1.0],[0.0,1.0,0.0],[1.0,0.0,3.0]])

solving = Jacoby(thre_b_thre, error=1e-18)

solving.calculate()

print(solving.matrix)








