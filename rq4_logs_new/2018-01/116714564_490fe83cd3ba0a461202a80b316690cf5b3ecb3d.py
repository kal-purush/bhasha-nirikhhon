#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
title           : diff_hell.py
description     : Diffie-Hellman Schlüsselaustausch
author          : kris
usage           : python diff_hell.py p n
parameter1      : Primzahl p
parameter2      : Basis g
[parameter3]    : Optional: Geheime Zufallszahl 1
[parameter4]    : Optional: Geheime Zufallszahl 2
example         : python diff_hell.py "1" "2" "3" "4"
example2        : python diff_hell.py "1" "2"
"""

import sys
import math
from random import randint

def is_prime(n):
    if n == 0 or n < 0:
        return False;
    if n == 1:
        return True;
    if n % 2 == 0 and n > 2: 
        return False
    return all(n % i for i in range(3, int(math.sqrt(n)) + 1, 2))

def diff_hell(p, g, z1=0, z2=0):
    if is_prime(p) == False:
        print("p muss eine Primzahl sein.")
        return;
    if z1 == 0:
        z1 = randint(1, p-2)
    if z2 == 0:
        z2 = randint(1, p-2)
    if z1 == z2:
        z2 = randint(1, p-2)
    
    alpha = pow(g, z1, p)
    beta = pow(g, z2, p)
    
    alice = pow(beta, z1, p)
    bob = pow(alpha, z2, p)
    
    if(alice == bob):
        print("Schlüsselaustausch erfolgreich!")
        print("Zufallszahl z1: " + str(z1))
        print("Zufallszahl z2: " + str(z2))
        print("alpha: " + str(alpha))
        print("beta: " + str(beta))
        print("Schlüssel: " + str(alice))
    else:
        print("irgendwas hässliches ist schief gegangen!")
    
    

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) < 5:
        print('Usage {0}: x y [z1] [z2]'.format(sys.argv[0]))
        print('Parameter p: Primzahl p')
        print('Paranter  g: Gemeinsame Basis g')
        print('Optional z1: Geheime Zufallszahl z1')
        print('Optional z2: Geheime Zufallszahl z2')
        sys.exit(1)
    p = int(sys.argv[1])
    g = int(sys.argv[2])
    if len(sys.argv) > 4: 
        z1 = int(sys.argv[3])
        z2 = int(sys.argv[4])
        diff_hell(p, g, z1, z2)
    elif len(sys.argv) > 3:
        diff_hell(p, g)
    else:
        print("{0}:{1}".format(sys.argv[0], "4 Parameter, siehe Docu")) 