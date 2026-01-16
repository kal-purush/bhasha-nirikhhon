from multiprocessing import Pool
from functools import partial
import numpy as n
from itertools import product as cartesian_product
from matplotlib import pyplot as plt
from numba import jit


def compute_prob_dist(J, h, N, q, n_procs=30):
    """Compute the probability distribtion over all states of given network
    Arguments:
        J {ndarray} -- array of couplings between nodes, shape (N,N,q,q)
        h {ndarray} -- array of local fields of shape (N,q)
        N {int} -- number of neurons
        q {int} -- number of states
    Returns:
        tuple -- a tuple containing S {ndarray}, an array of all possible states, Ps {ndarray}, normalized probability of all states, and Z {float}, the partition function
    """

    @jit(nogil=True, nopython=True)
    def H(s):
        """Calculates the hamiltonian of a given state
        Arguments:
            s {ndarray} -- array encoding the state of shape (N,)
            J {ndarray} -- array of couplings of shape (N,N,q,q)
            h {ndarray} -- array of local fields of shape (N,q)
            N {int} -- number of neurons
        Returns:
            ndarray -- The value of the Hamiltonian for given state
        """
        H = 0
        for i in range(N):
            for j in range(i):
                H -= J[i, j, s[i]-1, s[j]-1]
            H -= h[i, s[i]-1]
        return n.exp(-H)

    S = n.array(list(cartesian_product(*[list(range(q))]*N)), n.int8) + 1
    Hs = n.array([H(s) for s in S])
    Z = Hs.sum(axis=0)
    Ps = Hs / Z
    return S, Ps, Z
	
	
	
	
	def make_dataset(P, S, d=100000):

    samples = S[n.random.choice(range(S.shape[0]), size=d, replace=True, p=P)]

    return samples

def get_f_i_k(D, N, q):
    f_i_k = n.zeros((N, q), n.float32)
    for k in range(q):
        D_k = (D == (k+1)).astype(n.int)
        for i in range(N):
            f_i_k[i,k] = D_k[:,i].sum()
    return f_i_k / D.shape[0]


def get_f_ij_kl(D, N, q):

    f_ij = n.zeros((N, N, q, q), n.float32)
    for k in range(q):
        D_k = (D == (k+1)).astype(n.int)
        for l in range(q):
            D_l = (D == (l+1)).astype(n.int)
            for i in range(N):
                for j in range(N):
                    if i != j:
                        f_ij[i, j, k, l] = n.dot(D_k[:, i], D_l[:, j])
    return f_ij / D.shape[0]