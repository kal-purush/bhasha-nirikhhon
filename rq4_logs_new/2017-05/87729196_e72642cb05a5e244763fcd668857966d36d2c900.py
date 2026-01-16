import numpy as np

#Testing the nearly free electron model
#Calculates the energy value for a given lattice type (plv), direction (hql) and path (K)

def nrg(plv,K,h=1,q=1,l=1,plotPts=50,a=1) :
	hbar=1.05459E-34
	mass=9.1093835E-31

	Energy=np.linspace(0,1,plotPts)
	b1=(np.cross(plv[...,0],plv[...,1]))/np.dot(plv[...,0],np.cross(plv[1],plv[2]))
	b2=np.cross(plv[...,1],plv[...,2])/np.dot(plv[...,0],np.cross(plv[1],plv[2]))
	b3=np.cross(plv[...,2],plv[...,0])/np.dot(plv[...,0],np.cross(plv[1],plv[2]))
	G=np.array([ [(2*np.pi/a)*(h*b1[0]+q*b2[0]+l*b3[0]) ], [(2*np.pi/a)*(h*b1[1]+q*b2[1]+l*b3[1]) ], [(2*np.pi/a)*(h*b1[2]+q*b2[2]+l*b3[2])] ])

	i=0
	while i < plotPts :
		Energy[i] = hbar/(2*mass)*(np.linalg.norm(K[...,i])+np.linalg.norm(G))**2
		#Kpt[i]=np.linalg.norm(K[...,i])
		#print(Energy)
		i+=1

	return Energy