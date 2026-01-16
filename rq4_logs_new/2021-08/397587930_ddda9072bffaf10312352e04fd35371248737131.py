"""
In this script we calculate H_min(A|X) for the CGLMP_3 game constrained by some
CGLMP_3 winning probability.
"""

def ent(SDP):
    # Returns the entropy of the computed solution
    return -1 * log2(-SDP.dual)


import fractions
from itertools import product
from math import log2, pow

import ncpol2sdpa as ncp


# CGLMP game dimension 3
# X = Y = {0, 1}
# A = {0, 1, 2}
# B = {0, 1, 2}
# Probabilistic winning function
#  w(a,b,x,y,) = [a-b = xy mod 3] + 1/2 * [a-b = (-1)^{x \oplus y} + xy mod 3]
def game_pred(a, b, x, y):
    result = 0
    result += (a - b) % 3 == (x * y) % 3
    result += fractions.Fraction(((a - b) % 3 == (pow(-1, x ^ y) + x * y) % 3), 2)
    return result


# Global level of NPA relaxation
LEVEL = 2
# Maximum CGLMP score, calculated by myself using ncpol2sdpa_cglmp_3_winprob.py
WMAX = 0.8643567588466105

results = {}
WCGLMPs = [
    0.75, 0.76, 0.77, 0.78, 0.79, 0.8, 0.805, 0.81, 
    0.815, 0.82, 0.825, 0.83, 0.835, 0.84, 0.845, 0.85, 0.86, WMAX
]

for WCGLMP in WCGLMPs:
    results[str(WCGLMP)] = []

    A_config = [3, 3]
    B_config = [3, 3]
    # Measurement operators
    A = [Ax for Ax in ncp.generate_measurements(A_config, 'A')]
    B = [By for By in ncp.generate_measurements(B_config, 'B')]
    W = ncp.generate_operators('W', 3, hermitian=True)

    substitutions = {}
    moment_ineqs = []
    moment_eqs = []
    operator_eqs = []
    operator_ineqs = []
    localizing_monos = []  # op_eqs are processed last so need to add three Nones to end

    # Adding the constraints for the measurement operators
    substitutions.update(ncp.projective_measurement_constraints(A, B))

    # Defining the cglmp inequality
    cglmp_expr = 0
    for (a, b, x, y) in product([0, 1, 2], [0, 1, 2], [0, 1], [0, 1]):
        if a == 2 and b == 2:
            cglmp_expr += game_pred(a, b, x, y) * (1 - A[x][0] - A[x][1]) * (1 - B[y][0] - B[y][1])
        elif a == 2 and b != 2:
            cglmp_expr += game_pred(a, b, x, y) * (1 - A[x][0] - A[x][1]) * B[y][b]
        elif a != 2 and b == 2:
            cglmp_expr += game_pred(a, b, x, y) * A[x][a] * (1 - B[y][0] - B[y][1])
        else:
            cglmp_expr += game_pred(a, b, x, y) * A[x][a] * B[y][b]
    # every input has probability 1/4
    cglmp_expr /= 4.0

    # constraint on the cglmp score
    w_exp = WCGLMP
    score_con = [cglmp_expr - w_exp]

    # Commutation constraints for W (I think the projective msmt thing already includes ones for A,B)
    for w in W:
        for Ax in A:
            for a in Ax:
                substitutions.update({w * a: a * w})
        for By in B:
            for b in By:
                substitutions.update({w * b: b * w})

    # \sum W_{a,b} <= I_{R'}
    operator_ineqs += [1 - (W[0] + W[1] + W[2])]
    # positivity constraints for W_{a,b}
    operator_ineqs += [w for w in W]
    # We must specify localizing mmonomials for the constraints of the
    # problem but by specifying None ncpol2sdpa uses a default set
    localizing_monos += [None] * 11

    moment_equalities = moment_eqs[:]
    moment_inequalities = moment_ineqs[:] + score_con[:]
    operator_equalities = operator_eqs[:]
    operator_inequalities = operator_ineqs[:]

    # We now specify some extra monomials to include in the relaxation
    extra_monos = []
    for w in W:
        for Ax in A:
            for a in Ax:
                for By in B:
                    for b in By:
                        extra_monos += [a * b * w]
                extra_monos += [a * w]
        for By in B:
            for b in By:
                extra_monos += [b * w]

    result_sum = 0
    for x in range(2):
        # Objective function
        """obj = A[x][0]*B[y][0]*W[0] + \
            A[x][0]*B[y][1]*W[1] + \
            A[x][0]*(1-B[y][0]-B[y][1])*W[2] + \
            A[x][1]*B[y][0]*W[3] + \
            A[x][1]*B[y][1]*W[4] + \
            A[x][1]*(1-B[y][0]-B[y][1])*W[5] + \
            (1-A[x][0]-A[x][1])*B[y][0]*W[6] + \
            (1-A[x][0]-A[x][1])*B[y][1]*W[7] + \
            (1-A[x][0]-A[x][1])*(1-B[y][0]-B[y][1])*W[8]"""
        obj = A[x][0] * W[0] + \
              A[x][1] * W[1] + \
              (1 - A[x][0] - A[x][1]) * W[2]

        ops = ncp.flatten([A, B, W])
        sdp = ncp.SdpRelaxation(ops, verbose=1, normalized=True, parallel=0)
        sdp.get_relaxation(level=LEVEL,
                           equalities=operator_equalities,
                           inequalities=operator_inequalities,
                           momentequalities=moment_equalities,
                           momentinequalities=moment_inequalities,
                           objective=-obj,
                           substitutions=substitutions,
                           extramonomials=extra_monos,
                           localizing_monomials=localizing_monos)
        sdp.solve('mosek')
        print(
            f"For a clgmp score {w_exp} and input x={x} we find an sdp dual value of {sdp.dual} "
            f"and with that an entropy of {ent(sdp)}."
        )
        result_sum += ent(sdp)
    results[str(WCGLMP)] += [result_sum / 2.0]
print(results)