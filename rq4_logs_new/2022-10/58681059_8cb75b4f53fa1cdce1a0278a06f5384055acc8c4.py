#!/usr/bin/env python3

def f(c0=10162.5, ny=40, s=900, rate=10):
    c=c0
    r=(1+rate/100)**(1/12)
    for y in range(ny*12):
        c+=s
        c*=r
    return c