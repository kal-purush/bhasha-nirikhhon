from numpy import *

def gencylmesh(nx,ny,lx,ly): #generates a mesh on a cylinder surface with nx points in the aperiodic direction and ny in the periodic direction. perimeter of cylinder is ly, length of it is 2*lx
    yvals = linspace(0,ly,ny,endpoint = False)
    xvals = linspace(-lx,lx,nx)
    [X,Y] = meshgrid(xvals,yvals)
    dx = xvals[1]-xvals[0]
    dy = yvals[1]-yvals[0]
    X1 = reshape(X,X.size)
    Y1 = reshape(Y,Y.size)
    np = X.size
    nb = 2*yvals.size #both edges contribute to b
    
    p = zeros((np,2))
    g = zeros((np,4),dtype = int)
    
    j = 0
    for i in range(np):
        p[i,0] = X1[i]
        p[i,1] = Y1[i]
        if (X1[i] == xvals[0]):
            j = j+1
            g[i,1] = -1
        elif (X1[i] == xvals[-1]):
            j = j+1
            g[i,3] = -1
        else:
            g[i,1] = i-1 
            g[i,3] = i+1

        if Y1[i] == yvals[-1]:
            g[i,0] = i%nx
        elif Y1[i] == yvals[0]:
            g[i,2] = Y1.size - (nx-i%10)
        else:
            g[i,0] = i+nx
            g[i,2] = i-nx

    return p,g,dx,dy #p(np,2) has coordinates of each point, b(nb) lists addresses in p where boundary points are, g(np,4) lists neighbours on lattice. negative entries indicate that there is no neighbour in this direction
#In g, the neighbours are listed as (up, left, down, right)

