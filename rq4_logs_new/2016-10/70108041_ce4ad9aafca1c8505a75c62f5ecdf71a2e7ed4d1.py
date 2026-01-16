#coding: utf-8
'''
Felipe Mocruha Alcantara
NUSP: 8531949

'''

import sys
import numpy as np
import numpy.random as random
from numpy import random
from scipy.cluster.vq import kmeans,whiten
from scipy.linalg import norm,pinv,lstsq

class rbf(object):  
    
    def __init__(self, ninput, nout, nk):        

        self.ninput = ninput
        self.nout = nout
        self.nk = nk
        self.b = 0.5
        self.w = random.random((nk, nout))
        self.k = [random.uniform(0,1,ninput) for i in xrange(nk)]
        
    def gauss(self, x, c):
        return np.exp(-self.b * norm(x-c)**2)

    def calcula_vetor_rbf(self, entrada):
        h = np.zeros((len(entrada), self.nk),float)
        
        for ci,c in enumerate(self.k):
            for xi,x in enumerate(entrada):
                h[xi,ci] = self.gauss(c,x)
        #bias
        h = h.tolist()
        for i in xrange(len(h)):
            h[i].insert(0,1)
        
        return np.array(h)

    def train(self, entrada, saida):
        """
        Treina a rede utilizando k-means para 
        determinar os centros das rbfs e utiliza
        o método dos minimos quadrados para calcular
        o vetor de pesos.

        """
        #encontrando os centros com k-means
        wh = whiten(entrada)
        self.k,distortion = kmeans(wh,2)
        #calculando as rbfs
        h = self.calcula_vetor_rbf(entrada)
        #calculando os pesos
        self.w,residual,rank,s = lstsq(h,saida)
        
    def test(self,entrada):

        h = self.calcula_vetor_rbf(entrada)
        f = np.dot(h,self.w)
        s = [norm(i) for i in np.transpose(f)]
        p = s.index(max(s)) 
        q = s.index(min(s))
        s[p] = 1
        s[q] = 0
        
        return f

def classifica(f):
    s = []
    for i in f:
        if i[0] == max(i):
            s.append([1,0])
        else:
            s.append([0,1])
    return s
            
#discretiza os valores dos atributos do dataset
def transforma_binario(v):

    """

    YELLOW = 1; PURPLE = 0;
    LARGE = 1; SMALL = 0;
    STRETCH = 1; DIP = 0;
    ADULT = 1; CHILD = 0;
    T = 1; F = 0;

    """

    final  = []
    for i in v:
        aux = []
        for j in xrange(len(i)):
            
            if i[j] == 'YELLOW':
                aux.append(1)
            elif i[j] == 'PURPLE':
                aux.append(0)
            elif i[j] == 'LARGE':
                aux.append(1)
            elif i[j] == 'SMALL':
                aux.append(0)
            elif i[j] == 'STRETCH':
                aux.append(1)
            elif i[j] == 'DIP':
                aux.append(0)
            elif i[j] == 'ADULT':
                aux.append(1)
            elif i[j] == 'CHILD':
                aux.append(0)
            elif i[j] == 'T\n':
                aux.append([1,0])
            elif i[j] == 'F\n':
                aux.append([0,1])

        final.append(aux)

    return final

        
if __name__ == '__main__':

    #recebendo a entrada
    d = open(sys.argv[1]).readlines()
    data = [i.split(',') for i in d]
    
    #preparando a entrada para a rede
    b = transforma_binario(data)
    pares_es = []
    saida = []

    for i in b:
        saida.append(i.pop())
    
    for i in xrange(len(b)):
        pares_es.append((b[i],saida[i]))

    #bias
    for i in pares_es:
        i[0].insert(0,1)        

    #embaralhando as ordem das entradas
    random.shuffle(pares_es)

    #criando a rbf
    rede = rbf(4,2,2)
    rede.train([i[0] for i in pares_es], [i[1] for i in pares_es])

    #caso de teste [YELLOW,SMALL,DIP,ADULT] 
    #testes = [[1,1,1,0,1]]
    testes = [i[0] for i in pares_es]
    f = rede.test(testes)

    #determinando a classe dos casos de teste
    s = classifica(f)
    
    #avaliando o aprendizado
    na = 0
    ta = 0.0
    desejado = [i[1] for i in pares_es]
    for i in xrange(len(s)):
        if s[i] == desejado[i]:
            na += 1
    ta = len(s)/na
    print 'Taxa de aprendizado dos dados de entrada: ', float(ta)

    #Casos de teste
    print '=================================='
    print 'Teste 1:'
    print 'Entrada = [YELLOW,SMALL,DIP,ADULT]'
    print 'Saida esperada = TRUE'
    e1 = [[1,1,0,0,1]]
    f1 = rede.test(e1)
    s1 = classifica(f1)
    c1 = ''
    if s1[0] == [1,0]:
        c1 = 'TRUE'
    else:
        c1 = 'FALSE'
    print 'Saida da rede = ',c1
    print '=================================='

    print '=================================='
    print 'Teste 2:'
    print 'Entrada = [YELLOW,LARGE,DIP,CHILD]'
    print 'Saida esperada = FALSE'
    e2 = [[1,1,1,0,0]]
    f2 = rede.test(e2)
    s2 = classifica(f2)
    c2 = ''
    if s2[0] == [1,0]:
        c2 = 'TRUE'
    else:
        c2 = 'FALSE'
    print 'Saida da rede = ',c2
    print '=================================='

    #Casos de teste
    print '=================================='
    print 'Teste 3:'
    print 'Entrada = [PURPLE,SMALL,DIP,ADULT]'
    print 'Saida esperada = FALSE'
    e3 = [[1,0,0,0,1]]
    f3 = rede.test(e3)
    s3 = classifica(f3)
    c3 = ''
    if s3[0] == [1,0]:
        c3 = 'TRUE'
    else:
        c3 = 'FALSE'
    print 'Saida da rede = ',c3
    print '=================================='

    print '=================================='
    print 'Teste 4:'
    print 'Entrada = [YELLOW,SMALL,STRETCH,CHILD]'
    print 'Saida esperada = TRUE'
    e4 = [[1,1,0,1,0]]
    f4 = rede.test(e4)
    s4 = classifica(f4)
    c4 = ''
    if s4[0] == [1,0]:
        c4 = 'TRUE'
    else:
        c4 = 'FALSE'
    print 'Saida da rede = ',c4
    print '=================================='