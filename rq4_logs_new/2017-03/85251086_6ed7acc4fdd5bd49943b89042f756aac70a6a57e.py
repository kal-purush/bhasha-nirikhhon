#%matplotlib inline
import numpy as np
import math
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

def generate_linear_data(w,b,n):
    dim = len(w)
    y = []
    X = []
    for i in xrange(n):
        x = np.random.uniform(-10,10,dim)
        if(np.dot(w,x) + b > 5):
            y.append(1)
            X.append(x)
        elif(np.dot(w,x) + b < -5):
            y.append(-1)
            X.append(x)
    return np.array(X),y

X ,y = generate_linear_data([1,-3,-2],3,10)

X=np.array([[-2.81214997, -6.64093076,  0.94676252],
       [ 4.91337228,  1.29491011,  4.56116006],
       [ 8.18071064,  4.64529659, -5.9869538 ],
       [-7.81506569,  2.99811495, -9.7573152 ],
       [-6.85085531, -5.36402403,  9.15515526],
       [ 6.02907685,  6.33088307,  8.73496787],
       [ 1.84914425, -6.14994496,  7.99878312],
       [ 1.83980308,  3.6167442 ,  0.67667057],
       [-9.18944555, -5.10177485,  9.24538592]])

y=[1, -1, 1, 1, -1, -1, 1, -1, -1]

def plot_plane_and_points(X,y,w,b):
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    X1 = []
    X2 = []

    for i in xrange(len(y)):
        if (y[i] == -1):
            X1.append(X[i])
        else:
            X2.append(X[i])

    X1 = np.array(X1)
    X2 = np.array(X2)

    ax.scatter(X1[:,0], X1[:,1], X1[:,2], c='blue', marker='^')
    ax.scatter(X2[:,0], X2[:,1], X2[:,2], c='red', marker='o')

    # create x,y
    xx, yy = np.meshgrid(np.arange(-15,15), np.arange(-15,15))

    # calculate corresponding z
    z = (-w[0] * xx - w[1] * yy - b) * 1. /w[2]

    # plot the surface
    ax.plot_surface(xx, yy, z,alpha=0.3, color="green")

def generate_linear_data(w,b,n):
    dim = len(w)
    y = []
    X = []

    for i in xrange(n):
        x = np.random.uniform(-10,10,dim)

        if(np.dot(w,x) + b > 5):
            y.append(1)
            X.append(x)
        elif(np.dot(w,x) + b < -5):
            y.append(-1)
            X.append(x)

    return np.array(X),y

data = generate_linear_data(np.array([1,2,3]),0,1000)

w = np.array([ 0.47542968,  0.86705391,  1.43545253] )
b = -6.10673197214e-12

#plot_plane_and_points(data[0], data[1], w, b)
plt.show()


class PLA:
	
	def __init__(self): #Construtor
		self.W=np.zeros(1)
		self.V=np.zeros(1)
		self.B=0
		self.tol=0.001

	def fit(self, X, y):
		(N, D)=X.shape
		W=np.random.rand(D)
		
		i=0
		b=0

		while i<N:
			V=np.dot(W, X[i,:])+b #Produto Interno

			if np.sign(V)!=np.sign(y[i]):
				W+=y[i]*X[i,:]
				b+=y[i]
				i=0
			i+=1
	
		self.W=W
		self.B=b

	def predict(self, X):
		y_=np.dot(X, self.W)

		for i in xrange(len(y_)):
			if y_[i]<0:
				y_[i]=-1

			else:
				y_[i]=1

		return y_

	def score(self, X, y):
		resp=self.predict(X)
		cnt=.0

		for i, r in enumerate(resp):
			if r==y[i]:
				cnt+=1.0

		return cnt/len(y)

	def erro(self, X, y):
		tol=self.tol #tolerancia
		n=-2*X*math.SecH(V)*errTot/len(X) #learning rate

		ytgh=math.TanH(V) #Funcao de Erro

		errAnterior=10000
		errTot=(y-ytgh)**2 #Erro inicial
		varErr=1000 #Variancia do erro inicial

		while varErr>tol:
			varErr=abs(errTot-errAnterior)
			errAnterior=errTot

			gradErr=-2*X*math.SecH(V)*errTot #Gradiente do erro
			self.W=self.W-n*gradErr #Ajustando W com o learning rate

			self.V=np.dot(self.W, X[i,:])+self.B #Atualiza V em funcao de W

			ytgh=math.TanH(V)
			errTot=(y-ytgh)**2


if __name__ == '__main__':
	test=PLA()
	test.fit(X, y)
	test.erro(X, y)

	print('X='+ str(X))
	print('W='+ str(W))
	print('b='+ str(B))
