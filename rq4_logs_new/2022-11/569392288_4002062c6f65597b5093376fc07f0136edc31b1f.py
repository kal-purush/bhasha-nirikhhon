# This is the main library file with the code for the different classes.

import sympy as smp
from scipy.optimize import fsolve
from utils import print_template
import numpy as np
import re

class RBCmodel_steady_state:
    def __init__(self, endogenous_variables, exog_parmas, round_int=3, _ss=False):
        # Copy the dictionary over
        self.endogenous_variables = endogenous_variables
        self.exog_params = exog_parmas

        # Define endogenous variables through dictionary variables.

        if not _ss:
            endogenous_variables = self.transform_input(endogenous_variables, exog_parmas)

        try:
            print("Initialising variables...")
            self.l_supply = endogenous_variables["l_supply"]
            self.k_accumulation = endogenous_variables["k_accumulation"]
            self.k_demand = endogenous_variables["k_demand"]
            self.l_demand = endogenous_variables["l_demand"]
            self.equilibrium = endogenous_variables["equilibrium"]
            self.euler_eq = endogenous_variables["euler_eq"]
            self.price_level = endogenous_variables["price_level"]
            self.equation_list = [self.l_supply, self.k_accumulation, self.k_demand, self.l_demand, self.equilibrium,
                                  self.euler_eq, self.price_level]

        # Define endogenous variables via indexing if key mismatch arises, inform the user of the
        # specific equation (use for loop with tries?).
        except:
            raise Exception("Input format incorrect, please respecify equations.")

        # Define exogenous parameters.
        self.alpha = exog_parmas["alpha"]
        self.beta = exog_parmas["beta"]
        self.sigma = exog_parmas["sigma"]
        self.phi = exog_parmas["phi"]
        self.delta = exog_parmas["delta"]
        self.exog_params_list = [self.alpha, self.beta, self.sigma, self.phi, self.delta]


        # Define rounding integer.
        self.round_int = round_int

    def transform_input(self, endogenous_variables, exog_params):
        # read through all the variables.
        symbols = []
        for equation in endogenous_variables.values():
            for symbol in re.split("[** /* + ( )-]", equation):
                if symbol != "":
                    try:
                        int(symbol)
                    except:
                        symbols.append(symbol)

        symbols = list(set(symbols))

        symbol_dict = {"P_ss": "1", "E_ss": "1", "A_ss": "1"}
        for symbol in symbols:
            if symbol not in exog_params.keys():
                letter = symbol.split("_")[0]
                symbol_dict[symbol] = letter+"_ss"

        endogenous_variables_ss = {}
        for key, equation in endogenous_variables.items():
            for symbol in sorted(symbol_dict, reverse=True):
                if symbol in equation:
                    equation = equation.replace(symbol, symbol_dict[symbol])
            endogenous_variables_ss[key] = equation

        return endogenous_variables_ss

    def print_equations(self):
        for equation in self.equation_list:
            print(" –   ", equation)

    def calc_steady_state(self):
        # Define function for fsolve to optimise over.
        def f(symbs, exog):
            C_ss, L_ss, K_ss, I_ss, Y_ss, W_ss, R_ss = symbs[0], symbs[1], symbs[2], symbs[3], symbs[4], symbs[5], symbs[6]
            alpha, beta, sigma, phi, delta = exog[0], exog[1], exog[2], exog[3], exog[4]

            l_supply = eval(self.l_supply)
            k_accumulation = eval(self.k_accumulation)
            k_demand = eval(self.k_demand)
            l_demand = eval(self.l_demand)
            equilibrium = eval(self.equilibrium)
            euler_eq = eval(self.euler_eq)
            price_level = eval(self.price_level)

            return np.array([l_supply, k_accumulation, k_demand, l_demand, equilibrium, euler_eq, price_level])

        # Include initial guess with "if none"?
        symbs0 = np.ones(len(self.endogenous_variables))
        res = fsolve(f, symbs0, args=self.exog_params_list)

        # Need to have these so that they adjust to input.
        endogenous_symbols = ["C", "L", "K", "I", "Y", "W", "R"]
        res = dict(zip(endogenous_symbols, np.round(res, self.round_int)))
        return res


class DSGEmodel:
    def __init__(self, matrices, var_labels):
        try:
            self.A, self.B, self.C, self.D = matrices["A"], matrices["B"], matrices["C"], matrices["D"]
            self.F, self.G, self.H, self.J = matrices["F"], matrices["G"], matrices["H"], matrices["J"]
            self.K, self.L, self.N, self.M = matrices["K"], matrices["L"], matrices["N"], matrices["M"]
        except:
            raise Exception("Please enter all matrices correctly.")

        self.m = self.F.shape[1]
        self.n_endog = self.C.shape[1]
        self.k_exog = self.N.shape[1]
        self.l = self.C.shape[0]

        self.var_labels = var_labels

    def solve(self):
        import scipy
        from numpy import dot, eye, zeros, kron
        import sympy as sym
        from scipy.linalg import null_space, eig, pinv, solve

        self.C_plus = pinv(self.C)
        self.C_T = np.transpose(self.C)
        self.C_null = null_space(self.C_T)
        self.C_0 = np.transpose(self.C_null)

        ## SUB OUT THE RESHAPE PART
        self.Psi = np.vstack(((np.zeros((self.l - self.n_endog)*self.m).reshape(self.l - self.n_endog, self.m)),
                              self.F - dot(dot(self.J, self.C_plus), self.A)))


        self.Gamma = np.vstack((dot(self.C_0, self.A),
                                dot(dot(self.J, self.C_plus),self.B) - self.G + dot(dot(self.K, self.C_plus), self.A)))

        self.Theta = np.vstack((dot(self.C_0, self.B),
                                dot(dot(self.K, self.C_plus), self.B) - self.H))

        self.Xi = np.vstack((np.hstack((self.Gamma, self.Theta)),
                             np.hstack((eye(self.m), zeros((self.m, self.m))))))

        ## ZEROS IS IN WRONG FORMAT?
        self.Delta = np.vstack((np.hstack((self.Psi, zeros((self.m, self.m)))),
                                np.hstack((zeros((self.m, self.m)), eye(self.m)))))

        self.eigenvals, self.eigenvecs = eig(self.Xi, self.Delta)

        self.Xi_sorted_abs = np.sort(abs(self.eigenvals))
        self.Xi_sorted_index = np.argsort(abs(self.eigenvals))
        self.Xi_sorted_val = self.eigenvals[self.Xi_sorted_index]
        self.Xi_sorted_vec = np.array([self.eigenvecs[:, i] for i in self.Xi_sorted_index]).T

        self.selector = np.arange(0, self.m)
        self.Lambda = np.diag(self.Xi_sorted_val[self.selector])
        self.Omega = self.Xi_sorted_vec[self.m:2 * self.m, self.selector]

        self.P = np.real(dot(dot(self.Omega, self.Lambda), pinv(self.Omega)))

        self.R = -dot(self.C_plus, (dot(self.A, self.P) + self.B))

        self.V = np.vstack((np.hstack((kron(eye(self.k_exog), self.A), kron(eye(self.k_exog), self.C))),
                            np.hstack((kron(self.N.T, self.F) + kron(eye(self.k_exog), dot(self.F, self.P) + dot(self.J, self.R) + self.G),
                                       kron(self.N.T, self.J) + kron(eye(self.k_exog), self.K)))))

        self.LN_plus_M = np.matrix(dot(self.L, self.N) + self.M)

        ## REPLACE WITH .flatten()
        self.D = np.matrix(self.D)
        self.D_flat = self.D[:,0]
        for counter_i in range(1,self.D.shape[1]):
            self.D_col = self.D[:,counter_i]
            self.D_flat = np.vstack((self.D_flat,self.D_col))

        self.LN_plus_M_flat = self.LN_plus_M[:,0]
        for counter_i in range(1,self.LN_plus_M.shape[1]):
            self.LN_plus_M_col = self.LN_plus_M[:,counter_i]
            self.LN_plus_M_flat = np.vstack((self.LN_plus_M_flat,self.LN_plus_M_col))

        self.denom = np.vstack((self.D_flat, self.LN_plus_M_flat))

        self.QS_vec = -solve(self.V, self.denom)
        self.Q = np.reshape(np.matrix(self.QS_vec[0:self.m*self.k_exog, 0]), (self.m, self.k_exog), 'F')
        self.S = np.reshape(self.QS_vec[(self.m * self.k_exog):((self.m + self.n_endog) * self.k_exog), 0],
                            (self.n_endog, self.k_exog), 'F')
        self.W = np.vstack((np.hstack((eye(self.m), zeros((self.m, self.k_exog)))),
                            np.hstack((dot(self.R, np.linalg.pinv(self.P)), (self.S - dot(dot(self.R, np.linalg.pinv(self.P)),
                                                                                          self.Q)))), np.hstack((zeros((self.k_exog, self.m)), eye(self.k_exog)))))

        print("Success")

    def plot_imp(self, x, title):
        import matplotlib.pyplot as plt
        plt.figure(dpi=220, figsize=(10,4))
        plt.plot(x=np.arange(0,len(x)), y=x)
        plt.title(title)
        plt.show()

    def print_solved_matrices(self, dp=3):
        print("P:", np.round(self.P, dp))
        print("R:", np.round(self.R, dp))
        print("Q:", np.round(self.Q, dp))
        print("S:", np.round(self.S, dp))

    def shock(self, periods=80, size=-2, random=False, print_data=False):
        import matplotlib.pyplot as plt
        from math import ceil

        if all(self.R) == None:
            raise Exception("You first need to solve the model using .solve()")

        # Randomised shock.
        if random == True:
            z = np.random.normal(size=(self.N.T.shape))
            print("Shock size: ", z)
        else:
            z = np.zeros(shape=self.N.T.shape) + size

        x = np.zeros(shape=self.P.T.shape)

        x_hist = []
        y_hist = []
        z_hist = []

        # Calculate the impulse responses.
        for period in range(periods):
            if period > 0:
                z = self.N*z
            y = np.matmul(self.R, x) + np.matmul(self.S, z)
            x = self.P*x + self.Q*z
            x_hist.append(x)
            y_hist.append(y)
            z_hist.append(z)

        # Create the list of values to be plotted.
        vars_for_plot = []
        for var in range(self.P.shape[0]):
            vars_for_plot.append([float(x_hist[i][var]) for i in range(len(x_hist))])
        for var in range(self.R.shape[0]):
            vars_for_plot.append([float(y_hist[i][var]) for i in range(len(y_hist))])
        for var in range(self.N.shape[0]):
            vars_for_plot.append([float(z_hist[i][var]) for i in range(len(z_hist))])

        # Create the figure and axes.
        num_rows = ceil(len(vars_for_plot) / 2)
        fig, axes = plt.subplots(nrows=num_rows, ncols=2, figsize=(num_rows*3, num_rows*4), dpi=200)
        plt.tight_layout()
        if len(vars_for_plot) % 2 != 0:
            fig.delaxes(axes[num_rows-1, 2-1])

        # Create a specific counter for the plotting.
        axes_counter = []
        for i in range(num_rows):
            axes_counter.append(i)
            axes_counter.append(i)

        # Loop and plot the variables.
        for var, index, index_2, title in zip(vars_for_plot, axes_counter, range(len(axes_counter)), self.var_labels):
            axes[index, index_2 % 2].plot(var)
            axes[index, index_2 % 2].set_title(title)

        if print_data == True:
            for var, title in zip(vars_for_plot, self.var_labels):
                print(title)
                print(var, "\n")

    def simulate(self, periods=300, print_data=False, zshock=(0, 2), xshock=(6, 2)):
        global x_hist, y_hist, z_hist
        import matplotlib.pyplot as plt
        from math import ceil

        # Randomise the shock and the initial level of capital.
        z = np.random.normal(size=(self.N.T.shape), loc=zshock[0], scale=zshock[1])
        x = np.random.normal(size=(self.P.T.shape), loc=xshock[0], scale=xshock[1])

        x_hist = []
        y_hist = []
        z_hist = []

        # Calculate the impulse responses.
        for period in range(periods):
            if period > 0:
                z = self.N*z + np.random.normal(size=(self.N.shape), scale=0.5)
            y = np.matmul(self.R, x) + np.matmul(self.S, z)
            x = self.P*x + self.Q*z
            x_hist.append(x)
            y_hist.append(y)
            z_hist.append(z)

        # Create the list of values to be plotted.
        vars_for_plot = []
        for var in range(self.P.shape[0]):
            vars_for_plot.append([float(x_hist[i][var]) for i in range(len(x_hist))])
        for var in range(self.R.shape[0]):
            vars_for_plot.append([float(y_hist[i][var]) for i in range(len(y_hist))])
        for var in range(self.N.shape[0]):
            vars_for_plot.append([float(z_hist[i][var]) for i in range(len(z_hist))])

        # Create the figure and axes.
        num_rows = ceil(len(vars_for_plot) / 2)
        fig, axes = plt.subplots(nrows=num_rows, ncols=2, figsize=(num_rows*3, num_rows*4), dpi=200)
        plt.tight_layout()
        if len(vars_for_plot) % 2 != 0:
            fig.delaxes(axes[num_rows-1, 2-1])

        # Create a specific counter for the plotting.
        axes_counter = []
        for i in range(num_rows):
            axes_counter.append(i)
            axes_counter.append(i)

        # Loop and plot the variables.
        for var, index, index_2, title in zip(vars_for_plot, axes_counter, range(len(axes_counter)), self.var_labels):
            axes[index, index_2 % 2].plot(var)
            axes[index, index_2 % 2].set_title(title)

        if print_data == True:
            for var, title in zip(vars_for_plot, self.var_labels):
                print(title)
                print(var, "\n")