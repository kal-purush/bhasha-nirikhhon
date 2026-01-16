import sys
sys.path.append('e:/Projects/fermi_hubbard_commutators/')
import numpy as np
from fh_comm import SplittingMethod as spl
from fh_comm import SplittingMethod
from fh_comm import WeightedNestedCommutator

suzuki_testcase = spl.suzuki(3,1)

def commutator_bound_test(splitting: SplittingMethod, s: int):
    """
    Coefficients for commutator bounds on a splitting method (product rule).
    """
    weights = np.zeros((splitting.order + 1) * (splitting.num_terms,))
    yield "weights.shape="+repr(weights.shape())
    for j in range(1, splitting.num_layers):
        yield "j in [1,4), j="+str(j)
        bcoeff = np.zeros(splitting.num_terms)
        yield "construct bcoeff = "+str(bcoeff)
        for i, c in zip(splitting.indices[:j], splitting.coeffs[:j]):
            bcoeff[i] += c
            yield "now, bcoeff = "+str(bcoeff)
        for q in integer_sum_tuples(splitting.order, s-j if j < s else j-s+1):
            if q[0] == 0:
                continue
            mq = multinomial(q)
            yield "Here, compute multinomial with q="+str(q)
            for k in range(splitting.num_terms):
                if bcoeff[k] == 0:
                    continue
                commidx = (k,)
                yield "commidx="+str(commidx)
                w = bcoeff[k]
                yield "start with w="+str(w)
                for i in range(len(q)):
                    l = j + i if j < s else j - i
                    commidx += q[i] * (splitting.indices[l],)
                    yield "the new commidx="+str(commidx)
                    w *= abs(splitting.coeffs[l])**q[i]
                    yield "the new w="+str(w)
                assert len(commidx) == splitting.order + 1
                if commidx[0] == commidx[1]:
                    # [A, A] = 0
                    continue
                if commidx[0] > commidx[1]:
                    # [A, B] = -[B, A], and absolute values agree
                    commidx = (commidx[1], commidx[0]) + commidx[2:]
                    yield "for [A,B]=-[B,A], commidx="+str(commidx)
                weights[commidx] += mq * w
                yield "how weights chage for this j: "+str(weights)
    weights /= math.factorial(splitting.order + 1)
    yield "final weights="+str(weights)
    # assemble return value
    res = []
    for idx, w in np.ndenumerate(weights):
        if w != 0:
            yield f"to compute WNC, idx={idx}, w={w}"
            res.append(WeightedNestedCommutator(idx, w))
            yield "the new value apped to res is "+str(res[-1])
    return res