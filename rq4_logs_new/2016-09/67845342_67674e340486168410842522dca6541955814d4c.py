# -*- coding: utf-8 -*-

"""
. Algoritmo: pivotamento

para i de 0 até N - 1
    para j de i + 1 até N
        n1 recebe Aji
        pivot recebe Aii
        div recebe pivot / n1
        Aj recebe Aj - Ai*div

====================================

x recebe vetor(0)
para i de N - 1 até 0
    sum recebe 0
    para j de N até 0
        se i == j
            x = A[i][j]
        senão
            sum recebe sum + A[i][j]
    x[i] = A[i+1][j]

"""


def pivotamento(A):
    """A função recebe uma matriz A (n x n) e
    retorna a matriz zerada na diagonal inferior
    esquerda."""
    for i in xrange(len(A) - 1):
        for j in xrange(i + 1,len(A)):
            n1 = A[j][i]
            n2 = A[i][i]
            if n2 == 0:
                continue
            div = n1 / n2
            A[j] = sumVector(multVector(A[i], -div), A[j])
    return A


def multVector(v, x):
    newV = []
    for i in xrange(len(v)):
        newV += [v[i] * x]
    return newV

def sumVector(v1, v2):
    newV = []
    for i in xrange(len(v1)):
        newV += [v1[i] + v2[i]]
    return newV

def printMatrix(A):
    for i in A:
        print "|\t",
        for j in i:
            print "%.1f" %(j), "\t",
        print "\t|"

A = [[1.0, -1.0, 2.0, -1.0, -8.0], [1.0, 1.0, 1.0, 0.0, -2.0], [2.0, -2.0, 3.0, -3.0, -20], [1.0, -1.0, 4.0, -3.0, 4.0]]
printMatrix(A)
print("\n\n")
printMatrix(pivotamento(A))