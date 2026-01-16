import numpy as np
import matplotlib.pyplot as plt
import valueIteration

# --------------
# | 0 | 1 | 2 |
# | 3 | 4 | 5 |
# | 6 | 7 | 8 |
# -------------
# labelling of the tiles

SUCCESS_RATE = 0.8

class tile:
	def __init__(self):
		self.actions = {}

	def setActions(self, u, r, l, d):
		self.actions = {"right": r, "left": l, "up": u, "down": d}

A = ["right", "left", "up", "down"]

tiles = [tile() for i in range(9)]
tiles[0].setActions(-1, 1, -1, -1)
tiles[1].setActions(-1, 2, 0, 4)
tiles[2].setActions(-1, -1, 1, 5)
tiles[3].setActions(-1, -1, -1, -1)
tiles[4].setActions(1, -1, 3, -1)
tiles[5].setActions(2, -1, -1, 8)
tiles[6].setActions(3, 7, -1, -1)
tiles[7].setActions(-1, 8, 6, -1)
tiles[8].setActions(5, -1, 7, -1)

tau = np.zeros((9, 4, 9))
rho = np.zeros((9, 4))

for i in range(9):
	s = tiles[i]
	for j in range(len(A)):
		a = A[j]
		for k in range(9):
			#special case for absorb state
			if i==3:
				rho[i,j]=0
				if k==i:
					#prob/reward of starting at absorb state and ending there
					tau[i,j,k]=1.0
				else:
					tau[i,j,k]=0
			else:
				if s.actions[a]==-1:
					rho[i,j]=-6.0
				#prob/reward of staying in same state
				if i==k:
					#if theres a wall/barrier
					if s.actions[a] == -1:
						tau[i,j,k]=1.0
					else:
						tau[i,j,k]=0.2
				else:
					if s.actions[a]==k:
						tau[i,j,k]=0.8
						rho[i,j]=-2



V = valueIteration.valueIteration(9, 4, tau, rho, 0.75)
plt.imshow(V.reshape((3, 3)), cmap="hot")
plt.show()

def plotHeapMap(iterations):
	for i in range(1,iterations+1):
		V = valueIteration.valueIteration(9, 4, tau, rho, 0.75, i)
		plt.imshow(V.reshape((3, 3)), cmap="hot")
		plt.show()


#plotHeapMap(9)


