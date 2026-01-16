import hashlib
import ElliptischeKromme as EK
import modulo
import random

def stringtobin(string):
    return '0b' + ''.join(map(lambda l: bin(ord(l))[2:], string))


def find_order(P):
    C = P.kromme
    n = 1
    product = P
    while product != EK.NulPunt(C):
        product += P
        n += 1
    return n
    
    
def generate_key(curve, G):
    if G.kromme != curve:
        raise Exception('Punt {} ligt niet op kromme {}'.format(G, curve))
    n = find_order(G)
    randgen = random.SystemRandom()
    dA = randgen.randint(1, n - 1)
    QA = dA * G
    return dA, QA
    
    
def sign_message(message, curve, G, dA, hashmethod=hashlib.sha1):
    if G.kromme != curve:
        raise Exception('Punt {} ligt niet op kromme {}'.format(G, curve))
        
    n = find_order(G)
    modn = modulo.IntModP(n)
    hash_ = hashmethod()
    hash_.update(message)
    e = bin(int(hash_.hexdigest(), 16))
    Ln = n.bit_length()
    z = e[:Ln + 2]
    randgen = random.SystemRandom()
    r = 0
    s = 0
    while r == 0 or s == 0:
        k = randgen.randint(1, n - 1)
        curvepunt = k * G
        r = int(curvepunt.x) % n
        s = (int(modn(k).inverse())*(int(z, 2) + r*dA)) % n
    return r, s