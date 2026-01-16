import csv
# from recommendation_data import dataset
from math import sqrt
import pymysql
import pprint
try:
    import numpy
except:
    print ("This implementation requires the numpy module.")
    exit(0)

"""
@INPUT:
    R     : a matrix to be factorized, dimension N x M
    P     : an initial matrix of dimension N x K
    Q     : an initial matrix of dimension M x K
    K     : the number of latent features
    steps : the maximum number of steps to perform the optimisation
    alpha : the learning rate
    beta  : the regularization parameter
@OUTPUT:
    the final matrices P and Q
"""

matrix = [[]]

def matrix_factorization(R, P, Q, K, steps=5000, alpha=0.0002, beta=0.02):
    Q = Q.T
    for step in range(steps):
        for i in range(len(R)):
            for j in range(len(R[i])):
                if R[i][j] > 0:
                    eij = R[i][j] - numpy.dot(P[i,:],Q[:,j])
                    for k in range(K):
                        P[i][k] = P[i][k] + alpha * (2 * eij * Q[k][j] - beta * P[i][k])
                        Q[k][j] = Q[k][j] + alpha * (2 * eij * P[i][k] - beta * Q[k][j])
        eR = numpy.dot(P,Q)
        e = 0
        for i in range(len(R)):
            for j in range(len(R[i])):
                if R[i][j] > 0:
                    e = e + pow(R[i][j] - numpy.dot(P[i,:],Q[:,j]), 2)
                    for k in range(K):
                        e = e + (beta/2) * ( pow(P[i][k],2) + pow(Q[k][j],2) )
        if e < 0.001:
            break
    return P, Q.T


def import_dataset():
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='aanchal07', db='minor2',autocommit=True)
    cur = conn.cursor()
    conn.autocommit(True)

    #fetching distinct userid
    cur.execute("SELECT DISTINCT userid FROM results;")
    user_details_data = cur.fetchall()
    userid = []
    #print(user_details_data)
    for u in user_details_data:
        userid.append(u[0])

    #fetching distinct bookid
    cur.execute("SELECT DISTINCT bookid FROM results;")
    book_details_data = cur.fetchall()
    bookid = []
    for b in book_details_data:
        bookid.append(b[0])

    #print(len(userid))
    #print(len(bookid))  
    #print(userid.index(42))

    #making a matrix to fill user*book rating
    row = len(userid)
    col = len(bookid)
    matrix = [[0 for i in range(col)] for j in range(row)]
    numrows = len(matrix)    
    numcols = len(matrix[0]) 
    #print(numrows)
    #print(numcols)
    #print(matrix[row - 1][col-1])
    cur.execute("SELECT * FROM results;")
    rating_details_data = cur.fetchall()
    for r in rating_details_data:
        matrix[userid.index(r[0])][bookid.index(r[1])] = r[2]
    #print(userid[5])
    #print(bookid.index('0375759778'))
    #print(matrix[5][13])
    matrix = numpy.array(matrix)

    N = len(matrix)
    print(N)
    M = len(matrix[0])
    print(M)
    K = 2

    P = numpy.random.rand(N,K)
    Q = numpy.random.rand(M,K)

    nP, nQ = matrix_factorization(matrix, P, Q, K)
    nR = numpy.dot(nP, nQ.T)
    print("nP is\n" + str(nP))
    print("\nnQ is\n" + str(nQ))
    print("\nnR is\n" + str(nR))
    conn.close()


import_dataset()
