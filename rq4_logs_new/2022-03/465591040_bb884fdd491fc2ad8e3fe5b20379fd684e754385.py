#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 11 19:44:23 2020

@author: erikarivadeneira
"""
import numpy as np 
import matplotlib.pyplot as plt

#%%VALORES DE LOS PARAMETROS Y MATRICES A CONSIDERAR 
a=0.02#0.02
B=0.04#0.02#0.1
C=0.08#0.11#0.08
d=0.1#0.1#0.09
e=0.15#0.1#0.2
f=0.1#0.1#0.55
g=0.05#0.15#0.3 
A = np.array([[1, 1+d+a, 1, 1-C], 
              [(1-a+d)*e, 1-a+d+f, 1-a+d, 1-a+d-C], 
              [1+g+B, 1+d+g, 1+g, (1+g)*(1-C)],
              [1-B+e+a, 1-B+e+d+a, (1-B+e+a)*g, 1-B+C]])#Coeficientes de la matriz costo
A2=np.array([[0.11,0.2,0.3,0.4],[0.3,.2,.7,.3],[.3,.4,.5,.2],[.4,.3,.5,.1]])
#A2=np.array([[1, 1+d+a],[1-a+d, 1-a+d+f],[1+g+B, 1+d+g]])
#A2=np.array([[1-a+d, 1-a+d+f],[1+g+B, 1+d+g]])
b = np.array([1, 1,1,1])# coeficientes libres
c = np.array([1, 1,1,1])# coeficientes de la matriz de inigualdades
#%%CLASE DE FUNCIONES PARA METODO SIMPLEX
class ModeloLineal:  
    def __init__(self, A = np.empty([0,0]), b = np.empty([0,0]), c = np.empty([0,0]), minmax = "MAX"):
        self.A = A
        self.b = b
        self.c = c
        self.x = [float(0)] * len(c)
        self.minmax = minmax
        self.imprimeIter = True
        self.optimalValue = None
        self.transform = False
    #Añadiendo datos a la tabla de datos
    def addA(self, A):
        self.A = A
    def addB(self, b):
        self.b = b
    def addC(self, c):
        self.c = c
        self.transform = False    
    def setimprimeIter(self, imprimeIter):
        self.imprimeIter = imprimeIter       
    def printSoln(self): #funcion que imprime solucion
        print("Coeficientes: ")
        print(self.x)
        print("Valor óptimo: ")
        print(self.optimalValue)    
    def printTableau(self, tableau):
        print("ind \t\t", end = "")
        for j in range(0, len(c)):
            print("x_" + str(j), end = "\t")
        for j in range(0, (len(tableau[0]) - len(c) - 2)):
            print("s_" + str(j), end = "\t")
        print()
        for j in range(0, len(tableau)):
            for i in range(0, len(tableau[0])):
                if(not np.isnan(tableau[j, i])):
                    if(i == 0):
                        print(int(tableau[j, i]), end = "\t")
                    else:
                        print(round(tableau[j, i], 2), end = "\t")
                else:
                    print(end = "\t")
            print()
    def Tableau1(self):        # Construyendo tabla inicial
        if(self.minmax == "MIN" and self.transform == False):
            self.c[0:len(c)] = -1 * self.c[0:len(c)]
            self.transform = True
        t1 = np.array([None, 0])
        numVar = len(self.c)
        numSlack = len(self.A)
        t1 = np.hstack(([None], [0], self.c, [0] * numSlack))
        basis = np.array([0] * numSlack)     
        for i in range(0, len(basis)):
            basis[i] = numVar + i
        A = self.A        
        if(not ((numSlack + numVar) == len(self.A[0]))):
            B = np.identity(numSlack)
            A = np.hstack((self.A, B))            
        t2 = np.hstack((np.transpose([basis]), np.transpose([self.b]), A))        
        tableau = np.vstack((t1, t2))        
        tableau = np.array(tableau, dtype ='float')        
        return tableau
    #Función de optimización         
    def optimizar(self):
        if(self.minmax == "MIN" and self.transform == False):
            for i in range(len(self.c)):
                self.c[i] = -1 * self.c[i]
        tableau = self.Tableau1()
        if(self.imprimeIter == True):
            print("Tableau inicial:")
            self.printTableau(tableau) 
        #Asumiendo que la base inicial no es óptima
        optimal = False
        iter = 1 #Conteo de iteraciones
        while(True):    
            if(self.imprimeIter == True):
                print("----------------------------------")
                print("Iteración :", iter)
                self.printTableau(tableau)              
            if(self.minmax == "MAX"):
                for profit in tableau[0, 2:]:
                    if profit > 0:
                        optimal = False
                        break
                    optimal = True
            else:
                for cost in tableau[0, 2:]:
                    if cost < 0:
                        optimal = False
                        break
                    optimal = True#si todas las direcciones dan como resultado una disminución de las ganancias o un aumento del costo
            if optimal == True: 
                 break    # n-ésima variable entra en la base, cuenta para la indexación de tableau
            if (self.minmax == "MAX"):
                n = tableau[0, 2:].tolist().index(np.amax(tableau[0, 2:])) + 2
            else:
                n = tableau[0, 2:].tolist().index(np.amin(tableau[0, 2:])) + 2
            minimo = 99999 #Radio mínimo de testeo
            r = -1
            for i in range(1, len(tableau)): 
                if(tableau[i, n] > 0):
                    val = tableau[i, 1]/tableau[i, n]
                    if val<minimo: 
                        minimo = val 
                        r = i                       
            pivot = tableau[r, n]        
            print("Columna pivote:", n)
            print("Fila pivote:", r)
            print("Elemento pivote: ", pivot) #Realiza operaciones de pivoteo en filas
            tableau[r, 1:] = tableau[r, 1:] / pivot #dividiendo la fila pivote para el elemento pivote
            for i in range(0, len(tableau)): #Pivoteando filas nuevamente
                if i != r:
                    mult = tableau[i, n] / tableau[r, n]
                    tableau[i, 1:] = tableau[i, 1:] - mult * tableau[r, 1:] 
           #Nuevas variables básicas
            tableau[r, 0] = n - 2            
            iter += 1     
        if(self.imprimeIter == True):
            print("----------------------------------")
            print("Tabla final alcanzada en", iter, "iteraciones")
            self.printTableau(tableau)
        else:
            print("Resuelto!")  
        self.x = np.array([0] * len(c), dtype = float)
        #Guardando coeficientes
        for key in range(1, (len(tableau))):
            if(tableau[key, 0] < len(c)):
                self.x[int(tableau[key, 0])] = tableau[key, 1]
        self.optimalValue = -1 * tableau[0,1]
#%%PROBANDO METODO EN PARAMETROS
    
model1 = ModeloLineal()


model1.addA(A)
model1.addB(b)
model1.addC(c)

print("A =\n", A, "\n")
print("b =\n", b, "\n")
print("c =\n", c, "\n\n")
model1.optimizar()
print("\n")
print("Valores de los parámetros:\n")
print("a:",a)
print("b:",B)
print("c:",C)
print("d:",d)
print("e:",e)
print("f:",f)
print("g:",g)
model1.printSoln()

#%%METODO GRAFICO (SOLAMENTE SIRVE PARA 2 VARIABLES)
#c = np.array([-5.0, -4.0]) # Coeficientes de la funcion costo
#A = np.array([[6,4],[1,2],[-1,1],[0,1]]) # coeficientes de la matriz de inigualdades
#b = np.array([24, 6, 1, 2]) # coeficientes libres
a= 0.2
B= 0.1
C= 0.08
d= 0.09
e= 0.2
f= 0.55
g= 0.03
A2=np.array([[1, 1+d+a],[1-a+d, 1-a+d+f],[1+g+B, 1+d+g]])
b=np.array([1,1,1])
c=np.array([1,1])
from scipy.optimize import linprog
res = linprog(c, A2, b, method="revised simplex")
model1.addA(A2)
model1.addB(b)
model1.addC(c)
model1.printSoln()
def plot_constr(c,A,b,limites,zz):
    
    n = A.shape[0]
    print("Número de restricciones: ", n)
    plt.figure()
    plt.title("Costo:  z = "+ " + ".join([str(x)+" x"+str(i+1) for i,x in enumerate(c)]))
    X = np.linspace(0,15)
    for i in range(0,n):
        c_txt = " + ".join([str(x)+" x"+str(i+1) for i,x in enumerate(A[i,:])]) + " <= " + str(b[i])
        plt.plot(X, -X*(A[i,0]/A[i,1]) + b[i]/A[i,1] ,label = c_txt )         
    plt.plot(0.50761421, 0.38071066,"ro", label="Solucion")
    #plt.plot(X,-0.9125*X+0.6944,ls="--", label = "z=0.883248")
    plt.plot(X,-0.9143144*X+0.852857,ls="--", label = "z=0.883248")
    plt.xlim(0,limites[0])
    plt.ylim(0,limites[1])
    plt.legend()
    plt.grid()
    plt.show()
np.dot(res.x.T , -c)
plot_constr(c,A2,b,[1.2,1.2],[-1,0,5])
