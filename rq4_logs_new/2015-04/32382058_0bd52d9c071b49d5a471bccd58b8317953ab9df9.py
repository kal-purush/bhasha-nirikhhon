import numpy as np
import tebd
import gates
import ctm
import sys

J = -1
tau = 1e-3
chi = int(sys.argv[1])

p = 2
D = int(sys.argv[2])

kronecker = np.identity(D, dtype=float)
a = b = np.fromfunction(lambda j,k,l,m,n: kronecker[j,k]*kronecker[j,l]*kronecker[j,m]*kronecker[j,n], (p,D,D,D,D), dtype=int)
env = None

f = open("output/h_tau_mz_D={:d}_chi={:d}.dat".format(D, chi), "w")
f2 = open("output/h_a_D={:d}_chi={:d}.dat".format(D, chi), "w")
f3 = open("output/h_b_D={:d}_chi={:d}.dat".format(D, chi), "w")

"""
g = gates.imtime_evolution_from_pair_hamiltonian_pepo(
    -1. * gates.sigmax,
    -1. * gates.sigmazsigmaz,
    tau)

a, env, numit = tebd.itebd_square_pepo_invsymm(a, g, chi)
print numit

exit()
"""

for h in np.linspace(0, 4, 50):
    h = -h
    for tau in [1e-1, 1e-2, 1e-3]:
        print "[tfi_groundstate_2d] h={:.15e}; tau={:.15e}".format(h, tau)
        g1 = gates.exp_sigmax(-tau*h/2.0)
        g2 = gates.exp_sigmaz_sigmaz(-tau*J)
        gx = np.einsum(g2, [4,5,2,3], g1, [0,4], g1, [1,5])
        gy = np.einsum(g2, [0,1,4,5], g1, [2,4], g1, [3,5])
        a, b, env = tebd.itebd_square(a, b, gx, gy, chi, env=env)
    
        aDL = np.einsum(a, [8,0,2,4,6], a.conj(), [8,1,3,5,7]).reshape([D**2]*4)
        bDL = np.einsum(b, [8,0,2,4,6], b.conj(), [8,1,3,5,7]).reshape([D**2]*4)
        xDL = np.einsum(np.einsum(a, [5,1,2,3,4], gates.sigmaz, [5,0]), [8,0,2,4,6], a.conj(), [8,1,3,5,7]).reshape([D**2]*4).reshape(D**8)
        e = env.toarray1x1a(aDL, bDL).reshape(D**8)
        n = np.dot(e, aDL.reshape(D**8))
        mz = np.dot(e, xDL) / n
        f.write("{:.15e} {:.15e} {:.15e}\n".format(h, tau, mz))
        f.flush()
    
    f2.write("{:.15e} {:d} {:d}".format(h, p, D))
    for x in a.reshape(p*D**4):
        f2.write(" {:.15e}".format(x))
    f2.write("\n")
    f2.flush()
    
    f3.write("{:.15e} {:d} {:d}".format(h, p, D))
    for x in b.reshape(p*D**4):
        f3.write(" {:.15e}".format(x))
    f3.write("\n")
    f3.flush()

f.close()
f2.close()
f3.close()
