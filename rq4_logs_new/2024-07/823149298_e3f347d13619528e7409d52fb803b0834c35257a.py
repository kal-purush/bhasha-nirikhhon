import numpy as np
import scipy.sparse as sps
from scipy.fft import fft, fftfreq
import matplotlib.pyplot as plt
from matplotlib import cm
plt.close('all')
def sproll(M, k=1):
    return sps.hstack((M[:, k:], M[:, :k]), format="csr", dtype=float)
def L_mat(N,h):
    I = sps.eye(N,format = "csr", dtype = float)
    return (2*sproll(I,0) - sproll(I,1) - sproll(I,-1))/h**2
def L_mat_dir(N,h):
    return (2*sps.eye(N,format = "csr", dtype = float)- sps.eye(N,k=1,format = "csr", dtype = float) - sps.eye(N,k=-1,format = "csr", dtype = float))/h**2

def Finv(fu):
    return np.roll(fft(fu), -1, axis = 1)[:,::-1]



def defaultuinit(X, Y):
    return X*np.exp(-1*(X**2+Y**2))
class FourierKDV2D:
    def __init__(self, Nx, Ny, ax, bx, ay, by, dt, u0 = defaultuinit):
        """Setting frequencies"""
        self.xi =  fftfreq(Nx, (bx-ax)/Nx/2/np.pi)
        
        """Defining the grid"""
        self.Nx = Nx
        
        self.Ny = Ny
        self.xx, self.hx = np.linspace(ax, bx, Nx, retstep = True, endpoint = False)
        self.yy, self.hy = np.linspace(ay, by, Ny, retstep = True, endpoint = False)
        self.X, self.Y = np.meshgrid(self.xx, self.yy)
        
        self.zeros = np.zeros((Ny,1))
        
        self.A1 = sps.kron(sps.eye(self.Ny), sps.diags(self.xi[1:], 0, format = 'csr', dtype = np.complex128))
        self.A2 = sps.kron(sps.eye(self.Ny), sps.diags(self.xi[1:]**4, 0, format = 'csr', dtype = np.complex128))
        self.A3 = sps.kron(L_mat(self.Ny, self.hy), sps.eye(self.Nx-1))
        self.A4 = sps.kron(sps.eye(self.Ny), sps.diags(self.xi[1:]**2, 0, format = 'csr', dtype = np.complex128))
        
        
        self.M1 = 2*self.A1 + 1j*dt*(self.A2 - self.A3)
        self.M2 = 2*self.A1 - 1j*dt*(self.A2 - self.A3)
        self.M3 = 1j*dt/4*self.A4
        
        self.U0 = u0(self.X, self.Y)
        self.FU = fft(self.U0)/self.Nx
        self.eps = 1e-9
        self.itermax = 25
    def tick(self, dt):
        Vk = np.copy(self.FU)[:,1:].flatten()
        FUk = np.hstack((self.zeros, np.resize(Vk, (self.Ny, self.Nx-1))))
        res = 1 + self.eps
        k = 0
        while res > self.eps: 
            k+=1
            
            Vknew = sps.linalg.spsolve(self.M1, self.M2.dot(self.FU[:,1:].flatten()) + self.M3.dot(fft(Finv(FUk + self.FU)**2)[:,1:].flatten()/self.Nx))
        
            res = np.linalg.norm(Vk - Vknew)/((self.Nx-1)*self.Ny)
            Vk = np.copy(Vknew)
            FUk = np.hstack((self.zeros, np.resize(Vk, (self.Ny, self.Nx-1))))
            if k > self.itermax:
                print("itermax Reached")
                break
        self.FU = FUk
    def getU(self):
        return Finv(self.FU)
    def getFU(self):
        return np.copy(self.FU)
    def getX(self):
        return self.X
    def disp(self, time):
        U = np.real(self.getU())
        plt.figure("disp1")
        plt.clf()
        plt.title("U, t="+str(round(time,3)))
        plt.imshow(U[:,::-1], extent=[self.xx[0],self.xx[-1],self.yy[0],self.yy[-1]], aspect='auto')
        plt.xlabel("x")
        plt.ylabel("y")
        plt.show()
        plt.pause(0.01)
        
        fig = plt.figure(2)
        plt.clf()
        ax = fig.add_subplot(projection = '3d')
        surf = ax.plot_surface(self.X, self.Y, U[:,::-1], antialiased = True, rstride = 1, cstride = 1)
        fig.colorbar(surf, shrink=0.5)
        plt.show()
        plt.pause(0.1)
        
        # plt.figure("disp2")
        # plt.clf()
        # plt.title("U, t="+str(round(time,3)))
        # plt.plot(self.xx, U[self.Ny//2])
        # plt.xlabel("x")
        # plt.show()
        # plt.pause(0.01)

    
N = 300
l = 200
centerx = 0.5 
gamma = 1
lengthOfWave = 100
def soliton(c,X):
    return 3*c/(1 + np.sinh(0.5*np.sqrt(c)*X)**2)
def u1(X,Y):
    return soliton(3,np.sqrt(X**2+  gamma/lengthOfWave * Y**2))
def u2(X,Y):
    return soliton(3*(np.exp(-0.01*Y**2)),X)
    
dt = 1e-2
FKDV = FourierKDV2D(N, int(N/gamma), -l*centerx, l*(1-centerx), -l/2/gamma, l/2/gamma, dt,  u0 = u2)
for i in range(20000):
    if i%10==0:
        FKDV.disp(i*dt)
    FKDV.tick(dt)