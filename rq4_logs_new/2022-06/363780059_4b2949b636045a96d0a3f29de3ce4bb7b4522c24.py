"""
Construct various curves, with basepoint and orders

Examples:

    >>> E = Wei25519
    >>> g = E(*E.point)
    >>> print(E.order * g)
    [0: 1: 0]
    >>> E.pointorder * g == 0
    True

    >>> E = Curve25519
    >>> g = E(*E.point)
    >>> print(g)
    (9, _)
    >>> #print(E.basepoint_order * E.basepoint)
    (0, _)
"""
from numlib.quotient_rings import Zmodp
from numlib.elliptic_curves import Montgomery, Weierstrass

GF = Zmodp(2**255-19, negatives = True)

# This is the x-line of Curve25519; i.e., the curve modulo negation.
Curve25519 = Montgomery(GF(486662), GF(1))
type(Curve25519).pointorder = \
    0x1000000000000000000000000000000014def9dea2f79cd65812631a5cf5d3ed
type(Curve25519).order = type(Curve25519).pointorder * 8
type(Curve25519).point = (GF(9),)

# This Weierstrass curve is isomorphic to Curve25519; it is the whole
# curve, so not the quotient modulo negation.
Wei25519 = Weierstrass(
    a = GF(0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa984914a144),
    b = GF(0x7b425ed097b425ed097b425ed097b425ed097b425ed097b4260b5e9c7710c864)
)
type(Wei25519).pointorder = \
    0x1000000000000000000000000000000014def9dea2f79cd65812631a5cf5d3ed
type(Wei25519).order = type(Wei25519).pointorder * 8
type(Wei25519).point = (
    GF(0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad245a),
    GF(0x20ae19a1b8a086b4e01edd2c7748d14c923d4d7e6d7c61b229e9c5a27eced3d9)
)

if __name__ == "__main__":

    import doctest
    doctest.testmod()