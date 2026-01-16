import numpy as np
import flopy
import time
import h5py
from scipy.io import loadmat
from scipy.sparse.linalg import LinearOperator
from scipy.sparse.linalg import gmres
from scipy.sparse.linalg import cg

tic = time.clock()

##Simulation path       <<<<<<<<<<<<<[Input!] 
sim_name = 'After'
sim_path = 'C:/UHM/Research/Test'
exe_path = 'C:/UHM/Research/Upscaling/mf6'

##Hydraulic Conductivity <<<<<<<<<<<<<[Input!] 

# filepath= 'C:/UHM/Research/Upscaling/WD/relz.mat'
# f = h5py.File(filepath)
# mat = list(f['relz'])
# data = np.asarray(mat)
# Kdata = np.exp(5*data[1,:,:]).T

f1 = loadmat('C:/UHM/Research/Upscaling/WD8/K1.mat')
mat = list(f1['K1'])
Kdata = np.asarray(mat)**2



##Dimesion
d_up = 50        #<<<<<<<<<<<<<[Input!]     #Upscaled grid = d_up by d_up
d_fine = Kdata.shape[1]
N = int(d_fine/d_up)

##Iteration
#maxit = 100
####################[Upscaling]###############################
Kxx = np.zeros((d_up,d_up))
Kyy = np.zeros((d_up,d_up))
Kxy = np.zeros((d_up,d_up))

##Function
def spec2D(xx):
    xx = xx.reshape(-1,1)
    Ax =-xk*np.fft.fftn(K*np.fft.ifftn(np.reshape(xk.reshape(-1,1)*xx,(N,N))))-yk*np.fft.fftn(K*np.fft.ifftn(np.reshape(yk.reshape(-1,1)*xx,(N,N))))
    return Ax.reshape(-1)

##Upscaling
proc = 0
for i in range(d_up):
    for j in range(d_up):
        proc = proc + 1
        K = Kdata[N*i:N*(i+1),N*j:N*(j+1)]
        #Grid
        n = int(N/2)
        kk = np.asarray(list(range(0,n+1))+list(range(-n+1,0)))  #kk = [0:N/2-1 N/2 -N/2+1:-1] in matlab
        xk, yk = np.meshgrid(kk,kk)

        #Solve g1 & g2
        b1 = 1j*xk*np.fft.fftn(K)
        b2 = 1j*yk*np.fft.fftn(K)
       
        A = LinearOperator((N**2,N**2), matvec = spec2D)
         
        g1, exitcode1 = cg(A,b1.reshape(-1))
        if exitcode1 != 0:
            print("cg not converged: %d, %d, %s" % (exitcode1,proc,'g1'))
            g1, exitcode1 = gmres(A,b1.reshape(-1),x0=g1)
       # print('g1 convergence:', exitcode1)
        
        g2, exitcode2 = cg(A,b2.reshape(-1))
        if exitcode2 != 0:
            print("cg not converged: %d, %d, g2" % (exitcode1,proc))
            g2, exitcode2 = gmres(A,b2.reshape(-1),x0=g2)
        if proc % (2*d_up) == 0:
            print("Processing: [%d/%d]" % (proc,d_up**2))
        
        gm1 = g1.reshape(N,N)
        gm2 = g2.reshape(N,N)

        Kij = -0.5*np.mean(K*np.fft.ifftn(1j*yk*gm1+1j*xk*gm2))
        Kaa = -0.5*np.mean(K*np.fft.ifftn(2j*xk*gm1))
        Kbb = -0.5*np.mean(K*np.fft.ifftn(2j*yk*gm2))

        Kxx[i,j]= np.mean(K)+Kaa
        Kyy[i,j]= np.mean(K)+Kbb
        Kxy[i,j]= Kij
        

toc = time.clock()
print('Upscaling time:',toc-tic)

####################[Post-processing for MODFLOW6]###############################
K11 = np.zeros((d_up,d_up))
K22 = np.zeros((d_up,d_up))
Angle1 = np.zeros((d_up,d_up))

for i in range(d_up):
    for j in range(d_up):
        K_up = np.zeros((2,2))
        K_up[0,0] = Kxx[i,j]
        K_up[1,1] = Kyy[i,j]
        K_up[0,1] = Kxy[i,j]
        K_up[1,0] = Kxy[i,j]

        W,Vec = np.linalg.eig(K_up)
        idx = W.argsort()[::-1]   
        W2 = W[idx]
        Vec2 = Vec[:,idx]

        K11[i,j] = W2[0]
        K22[i,j] = W2[1]

        Angle1[i,j] = np.arctan2(Vec2[1,0],Vec2[0,0])* 180 / np.pi

####################[MODFLOW6]###############################
sim = flopy.mf6.MFSimulation(sim_name=sim_name, version='mf6', exe_name= exe_path, 
                             sim_ws=sim_path)

tdis = flopy.mf6.ModflowTdis(sim,pname='tdis', time_units='DAYS', nper=1,
                            perioddata=[(1.0,1,1.0)])

model_name='upscaling'
model = flopy.mf6.ModflowGwf(sim, modelname=model_name,
                             model_nam_file='{}.nam'.format(model_name))

ims_package = flopy.mf6.ModflowIms(sim, pname='ims', print_option='ALL',
                                   complexity='SIMPLE', outer_hclose=0.00001,
                                   outer_maximum=50, under_relaxation='NONE',
                                   inner_maximum=30, inner_hclose=0.00001,
                                   linear_acceleration='BICGSTAB',
                                   preconditioner_levels=7,
                                   preconditioner_drop_tolerance=0.01,
                                   number_orthogonalizations=2)


sim.register_ims_package(ims_package, [model_name])

dis_package = flopy.mf6.ModflowGwfdis(model, pname='dis', length_units='METERS',
                                      nlay=1,
                                      nrow=d_up, ncol=d_up, delr=N,
                                      delc=N,
                                      top=1.0, botm=-100.0,
                                      filename='{}.dis'.format(model_name))
# set the nocheck property in the simulation namefile
sim.name_file.nocheck = True
# set the print_input option in the model namefile
model.name_file.print_input = True

#Hydraulic conductivity
layer_storage_types = [flopy.mf6.data.mfdatastorage.DataStorageType.internal_array]
k_template = flopy.mf6.ModflowGwfnpf.k.empty(model, False, layer_storage_types, 100.0)
k_template['data'] = K11

k_template2 = flopy.mf6.ModflowGwfnpf.k.empty(model, False, layer_storage_types, 100.0)
k_template2['data'] = K22
#Angle
angle_template = flopy.mf6.ModflowGwfnpf.angle1.empty(model,False,layer_storage_types,0)
angle_template['data'] = -1.0*Angle1

angle_template2 = flopy.mf6.ModflowGwfnpf.angle1.empty(model,False,layer_storage_types,0)

angle_template3 = flopy.mf6.ModflowGwfnpf.angle1.empty(model,False,layer_storage_types,0)
# angle_template3['data'] = Angle3
#print(k_template)
# create npf package using the k template to define k
npf_package = flopy.mf6.ModflowGwfnpf(model, pname='npf', save_flows=True, icelltype=1,xt3doptions='active', k=k_template, k22 = k_template2,
                                     angle1 = angle_template)
                                    #  , angle2 = angle_template2, angle3 = angle_template3)

strt=[20]
ic_package = flopy.mf6.ModflowGwfic(model, pname='ic', strt=strt,
                                    filename='{}.ic'.format(model_name))
# #define initial heads for model to 20
# icPackage = flopy.mf6.ModflowGwfic(model,strt=20)

H_init = []
for i in range(d_up):
    a = [[0,i,0],10]
    H_init.append(a)
    b = [[0,i,d_up-1],0]
    H_init.append(b) 
# print(H_init)
stress_period_data1 = {0:H_init}
chdPackage = flopy.mf6.ModflowGwfchd(model, maxbound = 30, stress_period_data=stress_period_data1)

# set up Output Control Package
printrec_tuple_list = [('HEAD', 'ALL'), ('BUDGET', 'ALL')]
saverec_dict = {0:[('HEAD', 'ALL'), ('BUDGET', 'ALL')]}
oc_package = flopy.mf6.ModflowGwfoc(model, pname='oc', 
                                    budget_filerecord=[('{}.cbc'.format(model_name),)],
                                    head_filerecord=[('{}.hds'.format(model_name),)],
                                    saverecord=saverec_dict,
                                    printrecord=printrec_tuple_list)
# write simulation to new location 
sim.write_simulation()

# run simulation
sim.run_simulation()

#OUPUT
keys = sim.simulation_data.mfdata.output_keys()

import matplotlib.pyplot as plt

# get all head data
head = sim.simulation_data.mfdata['upscaling', 'HDS', 'HEAD']
# get the head data from the end of the model run
head_end = head[-1]
# plot the head data from the end of the model run
extent = (0.0, d_fine, 0.0 ,d_fine)
x = np.linspace(0,d_fine,d_up)
y = np.linspace(d_fine,0,d_up)
X,Y= np.meshgrid(x,y)

plt.figure(1)
plt.imshow(head_end[0,:,:],extent = extent)
plt.xlabel('X [m]',fontsize=13)
plt.ylabel('Y [m]',fontsize=13)
plt.title('Head of upscaled Hk (10x10) ',fontsize = 16)
cbar= plt.colorbar()
cbar.ax.set_ylabel('Head [m]',size = 13)
plt.contour(X,Y,head_end[0,:,:],levels = np.linspace(0,10,6), colors='k')

plt.savefig(sim_path + '/Head_up.png')

np.save(sim_path +'/head_up',head_end[0,:,:])