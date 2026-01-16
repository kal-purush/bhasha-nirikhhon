
import numpy as np
import pandas as pd
from scipy.linalg import toeplitz
from scipy.stats import invgamma

from functions_H import *



eta = 1
a = b = 1

N = 200
D = 7
Q = 4

num_datasets = 10


def xVector(C_q):
    x_1 = np.arange(D)
    x_cluster = x_1
    for i in range(C_q - 1):
        x_cluster = np.append(x_cluster, x_1)
    x_col = x_cluster.reshape((-1, 1))
    return x_col


def drawBeta_q(sigma2_q, k_q):
    cov_beta_q = sigma2_q * eta * np.eye(k_q)
    beta_q = np.random.multivariate_normal(np.repeat(0, k_q), cov_beta_q)
    return beta_q


def buildG_q(C_q, M_q, rho_q):
    A_q = eta * M_q

    R_1 = toeplitz(np.append(1, np.repeat(rho_q, D - 1)))
    R_q = np.zeros([D * C_q, D * C_q])
    for i in range(0, D * C_q, D):
        for j in range(0, D * C_q, D):
            if i == j:
                R_q[i:(i + D), i:(i + D)] += R_1

    G_q = A_q + R_q
    return G_q


def simulationOneCluster(C_q, M_q, sigma2_q, rho_q, k_q):
    beta_q = drawBeta_q(sigma2_q, k_q)
    G_q = buildG_q(C_q, M_q, rho_q)
    e_q = np.random.normal(0, 1, k_q)   # Do we use also Gumbel?
    Y_q = np.random.multivariate_normal(np.zeros(D * C_q), G_q)
    Y_q = Y_q + e_q
    return Y_q



def balancedDatasets(d):
    C_q = int(N / Q)

    x_q = xVector(C_q)
    M_q = poly_kernel(x_q, 2)       # What function to use?
    k_q = M_q.shape[1]

    x = np.empty(0)
    beta = np.empty(0)
    y = np.empty(0)

    for q in range(Q):
        sigma2_q = invgamma.rvs(a, b)
        rho_q = np.random.uniform(0, 1, 1)
        y_q = simulationOneCluster(C_q, M_q, sigma2_q, rho_q, k_q)

        x = np.append(x, x_q)
        y = np.append(y, y_q)

    id = np.repeat(np.arange(N), D)
    cluster = np.repeat(np.arange(Q), C_q * D)
    dataset = pd.DataFrame({'RID': id, 'Month': x, 'ADAS11': y, 'Cluster': cluster})
    filename = f'balanced_{d}.csv'
    dataset.to_csv(filename, index = False)



def unbalancedDatasets(d):
    Z = np.random.multinomial(N, [0.1, 0.2, 0.3, 0.4])

    x = np.empty(0)
    beta = np.empty(0)
    y = np.empty(0)
    cluster = np.empty(0)

    for q in range(Q):
        C_q = Z[q]

        if 0 == C_q:
            continue

        x_q = xVector(C_q)
        M_q = poly_kernel(x_q, 2)       # What function to use?
        k_q = M_q.shape[1]

        sigma2_q = invgamma.rvs(a, b)
        rho_q = np.random.uniform(0, 1, 1)
        y_q = simulationOneCluster(C_q, M_q, sigma2_q, rho_q, k_q)

        x = np.append(x, x_q)
        y = np.append(y, y_q)
        cluster = np.append(cluster, np.repeat(q, C_q * D))

    id = np.repeat(np.arange(N), D)
    dataset = pd.DataFrame({'RID': id, 'Month': x, 'ADAS11': y, 'Cluster': cluster})
    filename = f'unbalanced_{d}.csv'
    dataset.to_csv(filename, index = False)




for d in range(num_datasets):
    balancedDatasets(d)
    unbalancedDatasets(d)