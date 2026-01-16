# questão 1
import numpy as np
A1 = np.array([[1,2,-1,0],[2,3,4,3],[0,0,1,5],[-2,2,4,1]])

tracoA1 = np.trace(A1)
detA1 = np.linalg.det(A1)
print("QUESTAO 1 \n")
print("Traco A: ", tracoA1)
print("Determinante A: ", detA1)

B1 = np.array([[-2,3],[1,4]])
tracoB1 = np.trace(B1)
detB1 = np.linalg.det(B1)
print("Traco B: ", tracoB1)
print("Determinante B: ", detB1)

C1 = np.array([[3,1,-1],[1,0,2],[4,2,-1]])
tracoC1 = np.trace(C1)
detC1 = np.linalg.det(C1)
print("Traco C: ", tracoC1)
print("Determinante C: ", detC1)


# questão 2
print("\n\nQUESTAO 2 \n")

def calculaInversa(matriz):
    detMatriz = np.linalg.det(matriz)
    if(detMatriz!=0):
        return np.linalg.inv(matriz)
    elif(detMatriz==0):
        return "A matriz nao possui inversa, pois seu determinante e nulo."


A2 = np.array([[-1,2,3,-4],[4,2,0,0],[-1,2,-3,0],[2,5,3,1]])
B2= np.array([[2,-1],[-2,1]])
C2 = np.array([[-2,0,1],[3,1,-1],[2,1,0]])

invA = calculaInversa(A2)
invB = calculaInversa(B2)
invC = calculaInversa(C2)

print("Inversa de A: \n", invA)
print("Inversa de B: \n", invB)
print("Inversa de C: \n", invC)


# questão 3
print("\n\n QUESTAO 3:")
A3 = np.array([[-5,-3],[3,2]])
invA3 = calculaInversa(A3)
A3t = np.transpose(A3)
identidade2 = np.eye(2)
print("inv A3\n", invA3)
print("A3 transposta:\n", A3t)
print("Identidade 2x2\n", identidade2)
print("A-1 + At - I2:\n", invA3+ A3t - identidade2)