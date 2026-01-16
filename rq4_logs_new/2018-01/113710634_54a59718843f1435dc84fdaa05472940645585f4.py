#!/usr/bin/env python
# # -*- coding: utf-8 -*-

"""
The purpose of this script is to make simulations of insider trading.
Contents are based on the article :
    "Grorud, Axel & Pontier, Monique. (2011). Insider Trading in a Continuous
    Time Market Model. International Journal of Theoretical and Applied Finance"

Python version : 3.*
Authors :  Heang Kitiyavirayuth, Lucas Broux
"""

# Imports.
import math
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as st
from tqdm import tqdm


class SimpleModel:
    """
    This class proposes a simple model for simulation of insider trading.

    We suppose :
      - The market consists in one bond(r) and 2 assets (bi, sigma_i).
      - The insider knows the variable L = ln(S1(T)) - ln(S2(T)).
      - The utility function to optimize is logarithmic (U1 = U2 = ln).
    Then, according to the article, we can compute the wealth and consumption at time A < T by the following steps.
    """

    def __init__(self, parameters = 'default'):
        """
        Constructor for the class. Initialize variables of the class by setting them to false.
        """
        # Set seed for reproductibility.
        np.random.seed(42)

        # Parameters of the model.
        self.T = False                          # Total time (arbitrary unit).
        self.A = False                          # Time at which to compute health.
        self.n = False                          # Size of discretization.
        self.r = False                          # Interest rate of the bond.
        self.b1 = False                         # Drift of first asset.
        self.sigma_1 = False                    # Volatility of first asset
        self.b2 = False                         # Drift of second asset.
        self.sigma_2 = False                    # Volatility of second asset.
        self.x = False                          # Initial wealth.

        # Market evolution.
        self.market_evolution = False           # Evolution of the market as [[S0, S1, S2], [W1, W2]], each Si being Si = [Si(0), ... Si(T)] and Wi being brownian motion (time is discretized in n equal values between 0 and T).

        # Values that are computed during simulation.
        self.eta = False                        # Eta (constant vector).
        self.eta_square = False                 # The sqared norm of eta.
        self.gamma = False                      # Gamma (constant vector).
        self.l = False                          # l (2-D vector).
        self.B = False                          # New Brownian motion.
        self.M = False                          # Helper value (outsider agent).
        self.M_tilda = False                    # Helper value (insider agent).

        # Wealth and consumption of the insider.
        self.XA_insider = False                 # Wealth of the insider at time A, as [(A, wealth(A)), ...].
        self.c_insider = False                  # Consumption of the insider at time A, as [(A, consumption(A)), ...].
        self.critical_regions_insider = False   # Array of booleans whether the insider consumption is within the critical region or not.

        # Wealth and consumption of the outsider.
        self.XA_outsider = False                # Wealth of the outsider at time A, as [(A, wealth(A)), ...].
        self.c_outsider = False                 # Consumption of the outsider at time A, as [(A, consumption(A)), ...].
        self.critical_regions_outsider = False  # Array of booleans whether the outsider consumption is within the critical region or not.

        # Actually initialize parameters of the model if parameters == 'default'.
        if (parameters == 'default'):
            # Time input.
            self.T = 1                          # Total time (arbitrary unit).
            self.A = 0.9                        # Time at which to compute wealth.
            self.n = 500                        # Size of discretization.

            # Market parameters input.
            self.r = 0.1                        # Interest rate of the bond.
            self.b1 = 0.1                       # Drift of first asset.
            self.sigma_1 = 0.75                 # Volatility of first asset
            self.b2 = -0.05                     # Drift of second asset.
            self.sigma_2 = 1                    # Volatility of second asset.

            # Initial wealth of the agent.
            self.x = 1                          # Initial wealth.


    # Simulation of the brownian motion.
    def __W(self, t):
        """
        Simulation of the brownian motion W_tn.

        :param t: The array [t1, ... tn], tn being the highest value.
        :return: The array [W(t1n)m, ..., W(tn)m].
        """
        W = [0]
        for i in range(1, len(t)):
            W.append(W[-1] + np.random.normal(0, np.sqrt(t[i] - t[i-1])))
        return W


    # Simulation of market.
    def __market_2_assets(self, show = False):
            """
            Simulation of a market with 2 risky assets S1 and S2, and a bond S0.

            We create a regular discretization of the time in n equal values between 0 and T.

            :param show: Whether to show the plotted view.
            :return: The market as [[S0, S1, S2], [W1, W2]], each Si being Si = [Si(0), ... Si(T)] and Wi being brownian motion.
            """
            # Discretisation of time.
            time_steps = [i * self.T / self.n for i in range(self.n + 1)]

            # Compute 2 brownian motions.
            W1 = self.__W(time_steps)
            W2 = self.__W(time_steps)

            # Compute S0 (bond) and S1/S2 (risky assets) in the Black-Scholes model.
            S0 = np.exp(self.r * np.array(time_steps))
            S1 = np.exp( (self.b1 - self.sigma_1 * self.sigma_1 / 2) * np.array(time_steps) + self.sigma_1 * np.array(W1) )
            S2 = np.exp( (self.b2 - self.sigma_2 * self.sigma_2 / 2) * np.array(time_steps) + self.sigma_2 * np.array(W2) )

            # Plot evolution of assets if asked.
            if show:
                plt.figure(1)
                plt.plot(time_steps, S0, label = "Bond")
                plt.plot(time_steps, S1, label = "Risky asset S1")
                plt.plot(time_steps, S2, label = "Risky asset S2")
                plt.title("Representation of asset prices")
                plt.xlabel("Time")
                plt.ylabel("Asset prices")
                plt.legend(loc = 'best')
                plt.show()

            # Return result.
            return [[S0, S1, S2], [W1, W2]]


    # Simulation of the model as per the cited paper.
    def simulate(self):
        """
        Compute the optimal wealth of an insider and of a non-insider in a simplified case.

        We create a regular discretization of the time in n equal values between 0 and T.
        We suppose :
          - The market consists in one bond(r) and 2 assets (bi, sigma_i).
          - The insider knows the variable L = ln(S1(T)) - ln(S2(T)).
          - The utility function to optimize is logarithmic (U1 = U2 = ln).
        Then, according to the article, we can compute the wealth at time A < T by the following steps.
        We actualize the values that were set to False in the __init__ function.
        """
        ################################################
        # Step 1 : Compute eta, eta_square, and gamma. #
        ################################################
        self.eta = np.array([(self.b1 - self.r) / self.sigma_1, (self.b2 - self.r) / self.sigma_2])
        self.eta_square = np.dot(self.eta, self.eta)
        self.gamma = np.array([self.sigma_1, - self.sigma_2])

        #################################################
        # Step 2 : Simulate Brownian Motion and Market. #
        #################################################
        # Get market values over time.
        self.market_evolution = self.__market_2_assets()
        [[S0, S1, S2], [W1, W2]] = self.market_evolution

        ##########################################################
        # Step 3 : Compute 2-d vector l.                         #
        # We compute l on [0, ..., A] with the formula:          #
        # l(r) = 1 / (T - r) * int(gamma . dWs, [r, T]) * gamma. #
        ##########################################################
        considered_length = self.n
        self.l = [ ((self.gamma[0] * (W1[-1] - W1[i]) + self.gamma[1] * (W2[-1] - W2[i])) / (self.T * (1 - i / self.n))) * self.gamma for i in range(considered_length) ]

        #################################################
        # Step 4 : Compute B.                           #
        # We compute B on [0, ..., A] with the formula: #
        # B(t) = W(t) - int(l(u) . du, [0, t])          #
        #################################################
        self.B = [np.array([W1[i], W2[i]]) - sum(self.l[:(i)]) * (self.T / self.n) for i in tqdm(range(considered_length), desc = 'Computing Bt', leave = False)]

        ###################################
        # Step 5 : Compute M and M_tilda. #
        ###################################
        # Computation of M.
        self.M = [np.exp( - np.dot( self.eta, np.array([W1[i], W2[i]]) ) - .5 * (i * self.T / self.n) * self.eta_square ) for i in range(considered_length) ]

        # Computation of M_tilda.
        f = [v + self.eta for v in self.l]
        int_1 = np.array( [sum([np.dot(f[i_1], self.B[i_1 + 1] - self.B[i_1]) for i_1 in range(i_2)]) for i_2 in tqdm(range(considered_length), desc = 'Computing M_tilda', leave = False)] )
        int_2 = np.array( [(sum([np.dot(f[i_1], f[i_1]) for i_1 in range(i_2)]) * (self.T / self.n)) for i_2 in tqdm(range(considered_length), desc = 'Computing M_tilda', leave = False)] )
        self.M_tilda = np.exp(-int_1 - 0.5 * int_2)


    # Compute optimal strategy of agents, as per the cited paper.
    def compute_optimal_strategy(self):
        """
        Compute the optimal strategy of insider and outsider agents, according to the cited paper.
        """
        # Compute optimal health.
        corresponding_indice = int(self.A * self.n / self.T)
        # Optimal wealth at time A for the non-insider.
        y = self.x / (self.A + 1)
        self.c_outsider = [np.exp(self.r * (i *self.T / self.n)) * y / self.M[i] for i in range(corresponding_indice)]
        self.XA_outsider = self.c_outsider[-1] # In this model, optimal consumption equals optimal wealth.

        # Optimal wealth at time A for the insider.
        self.c_insider = [np.exp(self.r * (i *self.T / self.n)) * y / self.M_tilda[i] for i in range(corresponding_indice)]
        self.XA_insider = self.c_insider[-1] # In this model, optimal consumption equals optimal wealth.


    # Computation of critical retions, as per the cited paper.
    def __compute_critical_region(self, consumption):
        """
        Computes the statistical test for the given consumption process.

        :param consumption: The consumption of an agent, under the form of an array [(time, consumption(time))] of the consumption at different times.
        :return: Boolean array [b_1, ..., b_{n-1}] to attest whether the agent is in the critical region at time i.
        """
        # Compute Y_i = \log(R_{t_{i+1}} c_{t_{i+1}}) - \log(R_t C_t)
        # Where R_t = \exp(- r * t) is the discounting factor, and C_t is the consumption at time t.
        consecutive_consumption = [(consumption[i], consumption[i+1]) for i in range(len(consumption) - 1)]
        Y = [np.log(np.exp(-self.r * t1) * c1) - np.log(np.exp(-self.r * t0) * c0) for [(t0, c0), (t1, c1)] in consecutive_consumption]

        # Compute critical regions values.
        critical_regions = [(abs(np.log(np.exp(-self.r * t1) * c1) - np.log(np.exp(-self.r * t0) * c0) - 0.5 * (t1 - t0) * self.eta_square) > 1.96 * np.sqrt((t1 - t0) * self.eta_square)) for [(t0, c0), (t1, c1)] in consecutive_consumption]
        return critical_regions


    # Application of statistical test, as per the cited paper.
    def apply_statistical_test(self):
        """
        Apply the statistical test to the simulated values of consumption for both the insider and non-insider agent.
        """
        self.critical_regions_insider = self.__compute_critical_region([(i * self.T / self.n, self.c_insider[i]) for i in range(len(self.c_insider))])
        self.critical_regions_outsider = self.__compute_critical_region([(i * self.T / self.n, self.c_outsider[i]) for i in range(len(self.c_outsider))])



if __name__ == "__main__":
    # Initialize model.
    model = SimpleModel()
    # Simulate market.
    model.simulate()
    # Compute optimal strategies for agents.
    model.compute_optimal_strategy()
    # Apply statistical test.
    model.apply_statistical_test()
    # Print results.
    print(sum(model.critical_regions_outsider))
    print(sum(model.critical_regions_insider))