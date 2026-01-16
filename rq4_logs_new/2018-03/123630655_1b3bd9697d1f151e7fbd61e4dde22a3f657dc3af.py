
import math
from pyevtk.hl import gridToVTK
from pyevtk.hl import imageToVTK
from numpy import *; from numpy.linalg import *; import numpy as np
import matplotlib.pyplot as plt; from matplotlib import cm
from copy import deepcopy
import itertools
###### Flow definition #########################################################
maxIter = 37500 # Total number of time iterations.
Re      = 220.0  # Reynolds number.
nx = 520; ny = 182; ly=ny-1.0; q = 9 # Lattice dimensions and populations.
cx = nx/4; cy=ly/2; r=20;          # Coordinates of the cylinder.
uLB     = 0.01                       # Velocity in lattice units.
nulb    = 0.02#uLB*r/Re
omega = 1.0 / (3.*nulb+0.5) # Relaxation parameter.
scale = 4
param = 4
border = np.fromfunction(lambda x,y: x*0 > 0, (nx,ny))
frame = np.fromfunction(lambda x,y: x*0 > 0, (nx,ny))

###### Lattice Constants #######################################################
c = array([(x,y) for x in [0,-1,1] for y in [0,-1,1]]) # Lattice velocities.
t = 1./36. * ones(q)                                   # Lattice weights.
t[asarray([norm(ci)<1.1 for ci in c])] = 1./9.; t[0] = 4./9.
noslip = [c.tolist().index((-c[i]).tolist()) for i in range(q)]
i1 = arange(q)[asarray([ci[0]<0  for ci in c])] # Unknown on right wall.
i2 = arange(q)[asarray([ci[0]==0 for ci in c])] # Vertical middle.
i3 = arange(q)[asarray([ci[0]>0  for ci in c])] # Unknown on left wall.

i4 = arange(q)[asarray([ci[1]<0  for ci in c])] # Unknown on top wall.
i5 = arange(q)[asarray([ci[1]==0 for ci in c])] # horizontal middle.
i6 = arange(q)[asarray([ci[1]>0  for ci in c])] # Unknown on bottom wall.

###### Function Definitions ####################################################
sumpop = lambda fin: sum(fin,axis=0) # Helper function for density computation.

def equilibrium(rho,u, nx, ny):              # Equilibrium distribution function.
    cu   = 3.0 * dot(c,u.transpose(1,0,2))
    usqr = 3./2.*(u[0]**2+u[1]**2)
    feq = zeros((q,nx,ny))
    for i in range(q): feq[i,:,:] = rho*t[i]*(1.+cu[i]+0.5*cu[i]**2-usqr)
    return feq


class Grid:

    def __init__(self, nx, ny, main=False, R=np.eye(2), v=np.zeros(2), scale=1, fobstacle=lambda x,y:False):

        self.nx = nx
        self.ny = ny
        self.main_flag = main
        self.R = R
        self.v = v
        self.scale = scale


        vel_temp = np.dot(transpose(self.R), np.array([uLB ,0]))
        print(vel_temp)
        self.vel = fromfunction(lambda d,x,y: (1-d)*vel_temp[0] + d*vel_temp[1],(2,nx,ny))

        self.obstacle = np.fromfunction(lambda x,y: x*0 > 0, (nx,ny))
        for x in range(self.nx):
            for y in range(self.ny):
                temp = self.transofrm_to_main(np.array([x,y]))
                if fobstacle(temp[0], temp[1]):
                    self.obstacle[x,y] = True
                    # self.vel[:,x,y] = [0,0]
            #         print(x, y)
            # print("==============")
        # self.vel[:, 119, 50] = [uLB*2, 0]


        self.feq = equilibrium(1.0, self.vel, self.nx, self.ny);
        self.fin = self.feq.copy()
        self.rho = sumpop(self.fin)  # Calculate macroscopic density and velocity.
        self.u = dot(c.transpose(), self.fin.transpose((1, 0, 2))) / self.rho
        # self.snap("ahhahaha")
        self.u_pre = deepcopy(self.u)
        self.rho_pre = deepcopy(self.rho)


    def rotate_to_main(self, a):
        return np.dot(self.R, a);

    def transofrm_to_main(self, a):
        return self.rotate_to_main(np.dot(a, self.scale)) + self.v

    def update_border(self, gr):
        main_grid = deepcopy(gr)
        for x in range(self.nx):
            temp = self.transofrm_to_main([x, 0])
            x1 = math.floor(temp[0]); x2 = math.ceil(temp[0]); y1 = math.floor(temp[1]); y2 = math.ceil(temp[1])
            frame[x1, y1] = True; frame[x1, y2] = True; frame[x2, y1] = True; frame[x2, y2] = True; 
            if (x1 == x2 or y1 == y2):
                self.u[:,x,0] = np.dot(transpose(self.R), main_grid.u_pre[:,int(temp[0]), int(temp[1])])
                self.rho[x, 0] = main_grid.rho_pre[int(temp[0]), int(temp[1])]
            else:
                # print("sdaf")
                self.u[:, x, 0] = np.dot(transpose(self.R), (1./((x2-x1)*(y2-y1)))*(main_grid.u_pre[:, x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.u_pre[:, x2, y2]*(x1-temp[0])*(y1-temp[1])))
                self.rho[x,0] = (1./((x2-x1)*(y2-y1)))*(main_grid.rho_pre[x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.rho_pre[x2, y2]*(x1-temp[0])*(y1-temp[1]))



            temp = self.transofrm_to_main([x, self.ny-1])
            x1 = math.floor(temp[0]); x2 = math.ceil(temp[0]); y1 = math.floor(temp[1]); y2 = math.ceil(temp[1])
            frame[x1, y1] = True; frame[x1, y2] = True; frame[x2, y1] = True; frame[x2, y2] = True; 
            if (x1 == x2 or y1 == y2):
                self.u[:,x,-1] = np.dot(transpose(self.R), main_grid.u_pre[:,int(temp[0]), int(temp[1])])
                self.rho[x,-1] = main_grid.rho_pre[int(temp[0]), int(temp[1])]
            else:
                self.u[:, x, -1] = np.dot(transpose(self.R), (1./((x2-x1)*(y2-y1)))*(main_grid.u_pre[:, x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.u_pre[:, x2, y2]*(x1-temp[0])*(y1-temp[1])))
                self.rho[x,-1] = (1./((x2-x1)*(y2-y1)))*(main_grid.rho_pre[x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.rho_pre[x2, y2]*(x1-temp[0])*(y1-temp[1]))



            # temp = self.transofrm_to_main([x, 1])
            # self.u[:,x,1] = np.dot(transpose(self.R), main_grid.u[:,int(temp[0]), int(temp[1])])
            # self.rho[x,1] = main_grid.rho[int(temp[0]), int(temp[1])]
            # temp = self.transofrm_to_main([x, self.ny-2])
            # self.u[:,x,-2] = np.dot(transpose(self.R), main_grid.u[:,int(temp[0]), int(temp[1])])
            # self.rho[x,-2] = main_grid.rho[int(temp[0]), int(temp[1])]

        for y in range(self.ny):
            temp = self.transofrm_to_main([0, y])
            x1 = math.floor(temp[0]); x2 = math.ceil(temp[0]); y1 = math.floor(temp[1]); y2 = math.ceil(temp[1])
            frame[x1, y1] = True; frame[x1, y2] = True; frame[x2, y1] = True; frame[x2, y2] = True; 

            if (x1 == x2 or y1 == y2):
                self.u[:,0,y] = np.dot(transpose(self.R), main_grid.u_pre[:,int(temp[0]), int(temp[1])])
                self.rho[0,y] = main_grid.rho_pre[int(temp[0]), int(temp[1])]
            else:
                self.u[:, 0, y] = np.dot(transpose(self.R), (1./((x2-x1)*(y2-y1)))*(main_grid.u_pre[:, x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.u_pre[:, x2, y2]*(x1-temp[0])*(y1-temp[1])))
                self.rho[0,y] = (1./((x2-x1)*(y2-y1)))*(main_grid.rho_pre[x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.rho_pre[x2, y2]*(x1-temp[0])*(y1-temp[1]))

            temp = self.transofrm_to_main([self.nx-1, y])
            x1 = math.floor(temp[0]); x2 = math.ceil(temp[0]); y1 = math.floor(temp[1]); y2 = math.ceil(temp[1])
            frame[x1, y1] = True; frame[x1, y2] = True; frame[x2, y1] = True; frame[x2, y2] = True; 
            if (x1 == x2 or y1 == y2):
                self.u[:,-1,y] = np.dot(transpose(self.R), main_grid.u_pre[:,int(temp[0]), int(temp[1])])
                self.rho[-1,y] = main_grid.rho_pre[int(temp[0]), int(temp[1])]
            else:
                self.u[:, -1, y] = np.dot(transpose(self.R), (1./((x2-x1)*(y2-y1)))*(main_grid.u_pre[:, x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.u_pre[:, x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.u_pre[:, x2, y2]*(x1-temp[0])*(y1-temp[1])))
                self.rho[-1,y] = (1./((x2-x1)*(y2-y1)))*(main_grid.rho_pre[x1, y1]*(x2-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x2, y1]*(x1-temp[0])*(y2-temp[1]) - main_grid.rho_pre[x1, y2]*(x2-temp[0])*(y1-temp[1])  + main_grid.rho_pre[x2, y2]*(x1-temp[0])*(y1-temp[1]))



    def update_all(self, gr):
        grid = deepcopy(gr)

        for x in range(grid.nx):
            # for y in range(5, grid.ny-5):
            for y in itertools.chain(range(30), range(grid.ny-30,grid.ny)):
                rr = grid.v + grid.scale * np.dot(grid.R, np.array([x, y]))
                if y < 100:
                    rr = [math.ceil(rr[0]), math.ceil(rr[1])]
                else:
                    rr = [math.floor(rr[0]), math.floor(rr[1])]
                #map(int, rr)
                if frame[rr[0], rr[1]]:
                    continue
                ss = np.dot(1/grid.scale, np.dot(transpose(grid.R), np.array(rr) - grid.v))
                map(int,ss)
                map(int,ss)
                #if ss[0] <= 0 or ss[1] <= 0 or ss[0] >= grid.nx-1 or ss[1] >= grid.ny-1:
                #    continue
                self.u[:, int(rr[0]), int(rr[1])] = np.dot(grid.R, grid.u_pre[:, int(ss[0]), int(ss[1])])
                self.rho[int(rr[0]), int(rr[1])] = grid.rho_pre[int(ss[0]), int(ss[1])]
                border[int(rr[0]), int(rr[1])] = True

        # border_flag = False
        for y in range(grid.ny):
            for x in itertools.chain(range(30), range(grid.nx-30,grid.nx)):
                rr = grid.v + grid.scale * np.dot(grid.R, np.array([x, y]))
                if x < 100:
                    rr = [math.ceil(rr[0]), math.floor(rr[1])]
                else:
                    rr = [math.floor(rr[0]), math.ceil(rr[1])]
                #map(int, rr)
                if frame[rr[0], rr[1]]:
                    continue
                ss = np.dot(1/grid.scale, np.dot(transpose(grid.R), np.array(rr) - grid.v))
                map(int,ss)
                map(int,ss)
                #if ss[0] <= 0 or ss[1] <= 0 or ss[0] >= grid.nx-1 or ss[1] >= grid.ny-1:
                #   continue
                self.u[:, int(rr[0]), int(rr[1])] = np.dot(grid.R, grid.u_pre[:, int(ss[0]), int(ss[1])])
                self.rho[int(rr[0]), int(rr[1])] = grid.rho_pre[int(ss[0]), int(ss[1])]
                border[int(rr[0]), int(rr[1])] = True


    def update_all_all(self, gr):
        grid = deepcopy(gr)

        for x in range(grid.nx):
            for y in range(grid.ny):
            # for y in itertools.chain(range(5, 15), range(grid.ny - 15, grid.ny - 5)):
                rr = grid.v + grid.scale * np.dot(grid.R, np.array([x, y]))
                rr = [round(rr[0], 0), round(rr[1], 0)]
                map(int, rr)
                ss = np.dot(1/grid.scale, np.dot(transpose(grid.R), np.array(rr) - grid.v))
                ss = [round(ss[0], 0), round(ss[1], 0)]
                map(int,ss)
                if ss[0] <= 0 or ss[1] <= 0 or ss[0] >= grid.nx-1 or ss[1] >= grid.ny-1:
                   continue
                self.u[:, int(rr[0]), int(rr[1])] = np.dot(grid.R, grid.u_pre[:, int(ss[0]), int(ss[1])])
                self.rho[int(rr[0]), int(rr[1])] = grid.rho_pre[int(ss[0]), int(ss[1])]
                # border[int(rr[0]), int(rr[1])] = True

                    # print("=============")

    def snap(self, name):
        plt.clf();
        plt.imshow(sqrt(self.u[0] ** 2 + self.u[1] ** 2).transpose(), cmap=cm.coolwarm)
        plt.savefig("vel" + name + ".png")
        temp = sqrt(self.u[0] ** 2 + self.u[1] ** 2)
        # print(temp[119,82])

        vel_y = np.dstack((self.u[1, :, 1:-1], self.u[1, :, 1:-1]))
        vel_x = np.dstack((self.u[0, :, 1:-1], self.u[0, :, 1:-1]))
        magn = np.dstack((temp[:, 1:-1], temp[:, 1:-1]))

        if self.nx != nx:
            imageToVTK("./test" + name, pointData={"vel_x": vel_x, "vel_y": vel_y, "magnitude":magn})
        else:
            vfunc = np.vectorize(lambda x: int(x))
            fframe = vfunc(frame)
            fframe = fframe[:,1:-1]
            fframe = np.dstack((fframe, fframe))
            bborder = vfunc(border)
            bborder = bborder[:,1:-1]
            bborder = np.dstack((bborder, bborder))
            imageToVTK("./test" + name, pointData={"vel_x": vel_x, "vel_y": vel_y, "magnitude":magn, "frame":fframe, "border":bborder})



    def save_to_file(self):
        with open("test_data.csv", "w") as f:
            f.write(", ".join(map(str, self.u[0, int(nx/4), :])) + "\n")
            f.write(", ".join(map(str, self.u[1, int(nx/4), :])) + "\n")
            f.write(", ".join(map(str, self.u[0, int(nx/4)+20, :])) + "\n")
            f.write(", ".join(map(str, self.u[1, int(nx/4)+20, :])) + "\n")
            f.write(", ".join(map(str, self.u[0, int(nx/4)+30, :])) + "\n")
            f.write(", ".join(map(str, self.u[1, int(nx/4)+30, :])) + "\n")
            f.write(", ".join(map(str, self.u[0, int(nx/4)+40, :])) + "\n")
            f.write(", ".join(map(str, self.u[1, int(nx/4)+40, :])) + "\n")
            f.write(", ".join(map(str, self.u[0, int(nx/2), :])) + "\n")
            f.write(", ".join(map(str, self.u[1, int(nx/2), :])) + "\n")


class Chimera:

    def __init__(self):

        self.grids = []

    def add_grid(self, nx, ny, main=False, R=np.eye(2), v=np.zeros(2), scale=1, fobstacle=lambda x,y:False):
        g = Grid(nx, ny, main, R, v, scale, fobstacle)
        self.grids.append(g)


    def iteration(self):
        

        self.grids[0].fin[i1, -1, :] = self.grids[0].fin[i1, -2, :]  # Right wall: outflow condition.
        for i, grid in enumerate(self.grids):
            temp = 1
            if i == 1:
                temp = param#int(1/grid.scale)
            for k in range(temp):
                # print(i, k)
                grid.rho = sumpop(grid.fin)  # Calculate macroscopic density and velocity.
                grid.u = dot(c.transpose(), grid.fin.transpose((1, 0, 2))) / grid.rho

                if i == 0:
                    grid.u[:, 0, :] = grid.vel[:, 0, :]  # Left wall: compute density from known populations.
                    grid.rho[0, :] = 1. / (1. - grid.u[0, 0, :]) * (sumpop(grid.fin[i2, 0, :]) + 2. * sumpop(grid.fin[i1, 0, :]))
                    grid.update_all(self.grids[1])
                if i == 1:
                    grid.update_border(self.grids[0])


                grid.feq = equilibrium(grid.rho, grid.u, grid.nx, grid.ny)  # Left wall: Zou/He boundary condition.
                if i == 0:
                    grid.fin[i3, 0, :] = grid.feq[i3, 0, :]
                    grid.fin[:, border] = grid.feq[:, border]
                if i == 1:
                    grid.fin[:, 0, :] = grid.feq[:, 0, :]
                    grid.fin[:, -1, :] = grid.feq[:, -1, :]
                    grid.fin[:, :, 0] = grid.feq[:, :, 0]
                    grid.fin[:, :, -1] = grid.feq[:, :, -1]


                omega = 1.0 / (3. * nulb + 0.5)
                if i == 1:
                    omega = 2*omega / (8 - 3*omega)

                fout = grid.fin - omega * (grid.fin - grid.feq)  # Collision step.
                for ii in range(q): fout[ii, grid.obstacle] = grid.fin[noslip[ii], grid.obstacle]
                for ii in range(q):  # Streaming step.
                    grid.fin[ii, :, :] = roll(roll(fout[ii, :, :], c[ii, 0], axis=0), c[ii, 1], axis=1)

            grid.u_pre = deepcopy(grid.u)
            grid.rho_pre = deepcopy(grid.rho)

        # self.grids[0].update_all(self.grids[1])


            # for k in range(int(1/grid.scale)):
            #     grid.iteration()
            # grid.iteration()
        #self.grids[1].update_border(self.grids[0])

    def snap(self, num):
        # self.grids[0].snap("0-" + str(int(num)).zfill(3) + "a")
        # self.grids[0].update_all(self.grids[1])
        for i, grid in enumerate(self.grids):
            grid.snap(str(int(i)) + "-" + str(int(num)).zfill(3))
        self.grids[0].update_all_all(self.grids[1])
        self.grids[0].snap("_sum_"+str(int(i)) + "-" + str(int(num)).zfill(3))
        self.grids[0].save_to_file()




cylinder = lambda x,y: ((x-cx)**2+(y-cy)**2<r**2) | (y == 0) | (y == ly)
prism = lambda x,y: (abs(x-cx) + abs(y-cy) < r) | (y==0) | (y==ly)
g = Chimera()
g.add_grid(nx,ny,fobstacle=prism, main=True)
g.add_grid(320, 320, fobstacle=prism, scale=1/scale, v=np.array([130-40*sqrt(2), ly/2]), R=np.array([[math.cos(math.pi/4), math.sin(math.pi/4)], [-math.sin(math.pi/4), math.cos(math.pi/4)]]))
g.grids[1].update_border(g.grids[0])

temp = 250
for time in range(maxIter+1):
    print("Iteration", time)
    g.iteration()
    if (time%temp==0): g.snap(time/temp)
