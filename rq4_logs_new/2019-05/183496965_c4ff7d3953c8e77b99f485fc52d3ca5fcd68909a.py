#%%Solving the Heat Equation with changing boundary conditions using a 
# simple Explicit Method to Simulating a block of ice floating in seawater

# 1D Heat Equation solver for block of ice floating on seawater using a
# forward time centered space (FTCS) finite difference method

#import needed libbraries
import numpy as np
import matplotlib.pyplot as plt
from scipy import sparse
from scipy.sparse import linalg, diags

import matplotlib as mpl
mpl.rcParams.update(mpl.rcParamsDefault)


#%% Constants in SI Units
alpha = 0.3; # albedo
eps_ice = 0.96; # emissivity
rho_i = 916.7; # density of ice, kg/m^3
T_w = 272.15; # temperature of bulk water, K
c_pi = 2027; # specific heat of ice, J/(kgK)
kappa_ice = 2.25; # thermal conductivity of ice, W/(mK)
alpha_ice = (kappa_ice)/(c_pi*rho_i); # thermal diffusivity of ice, m^2/s

# Define function to calculate temperature based on time
def air_temp(t): #t is in seconds, so dt*i would be evaluated
    t_hours = (t/3600.0)%24 #convert to 24 hour clock
    temp = 7.0*np.sin((np.pi/12.0)*(t_hours-13.0))+268
    return temp
# the initial value for temperature would be air_temp(0.0)

#%% Initial and Boundary Conditions

# Space mesh
L = 2.0; # depth of sea ice
n = 400; # number of nodes
dx = L/n; # length between nodes
x = np.linspace(0.0,L,n+1);



#ND Parameters
diff_time_scale = (float(L**2))/(alpha_ice) #in seconds

#%% More numerical parameters

#some inital profile
def T_init(x):
    return -14.0*np.sin(np.pi*x/L) + ((air_temp(0.0)*(L-x))+(T_w*x))/L


# Time parameters
dt = [0.02, 0.5, 1, 2, 5, 100]; # time between iterations, in seconds
r_list = []
explicit_at_3000s_2cm = []

for k in range(0, 6):

    nt = int(20/dt[k]); # amount of iterations
    t_days = (dt[k]*nt)/86400.0
    
    # Calculate r, want ~0.25, must be < 0.5 (for explicit method)
    r = ((alpha_ice)*(dt[k]))/(dx*dx);# stability condition
    r_list.append(r)
    print("The value of r is ", r)
    
    
    u_exp = T_init(x)
    #set initial BC as well
    u_exp.shape = (len(u_exp),1)
    u_exp[0]=air_temp(0.0)
    u_exp[-1]=273.15
    
    #set initial conditions to the matrix as the first row
    u_exp_soln = u_exp
    
    
    
    #Create an empty list for outputs and plots
    top_ice_temp_list_exp = []
    air_temp_list_exp = []

    
    
    #%% Start Iteration and prepare plots
    
    for i in range(0,nt):
        
        # time in seconds to hours on a 24-hour clock will be used for air temp function
        print(f"i={i}/{nt}, %={(i/nt)*100:.3f}, hr={(i*dt[k]/3600)%24:.4f}")
        
        # Run through the FTCS with these BC
    #    Tsoln[1:n] = Tsoln[1:n]+r*(Tsoln_pr[2:n+1]-2*Tsoln_pr[1:n]+Tsoln_pr[0:n-1])   
        for j in range(1,n):
            u_exp[j] = u_exp[j] + r*(u_exp[j+1]-2*u_exp[j]+u_exp[j-1])
        
        #force to be column vector
        u_exp.shape = (len(u_exp),1)
    
        #update u top boundary
        u_exp[0]=air_temp(i*dt[k])
        
        # Now add the surface temp to its list
        top_ice_temp_list_exp.append(float(u_exp[0]))
        
        #append this array to solution file
        if (i*dt[k])%120 == 0: #every 60 seconds
            u_exp_soln = np.append(u_exp_soln, u_exp, axis=1)
        
    # write the solution matrix to a file
    u_exp_soln = u_exp_soln.transpose()
    np.savetxt(f"ex_output_{n+1}_nodes.txt",u_exp_soln, fmt = '%.10f',delimiter=' ')
    explicit_at_3000s_2cm.append(u_exp[40])





#%% Start Iteration and prepare plots

implicit_at_3000s_2cm = []

for k in range(0, 6):

    nt = int(20/dt[k]); # amount of iterations
    t_days = (dt[k]*nt)/86400.0
    
    # Calculate r, want ~0.25, must be < 0.5 (for explicit method)
    r = ((alpha_ice)*(dt[k]))/(dx*dx);# stability condition
    print("The value of r is ", r)
    
    #%% Set up the scheme

    #Create the matrices in the C_N scheme
    #these do not change during the iterations
    
    A_imp = sparse.diags([-r, 1+2*r, -r], [-1, 0, 1], shape = (n+1,n+1), format='lil')
    
    #now we pad the matrices with one in the TL and BR corners
    A_imp[0,[0,1]] = [1.0,0.0]
    A_imp[1,0] = -r
    A_imp[n,[n-1,n]] = [0.0,1.0]
    A_imp[n-1,n] = -r
    
    A_imp = A_imp.tocsc()
    
    
    u_imp = T_init(x)
    #set initial BC as well
    u_imp.shape = (len(u_imp),1)
    u_imp[0]=air_temp(0.0)
    u_imp[-1]=273.15
    
    # Now we have a initial distribution of temperature in the sea ice
    #plt.plot(x,Tsoln_pr,"g-",label="Initial Profile")
    #plt.title("Initial Distribution of Temperature in Sea Ice")
    #plt.close()
    
    #Create an empty list for outputs and plots
    top_ice_temp_list_imp = []
    air_temp_list_imp = []

    
    #set initial conditions to the matrix as the first row
    u_imp_soln = u_imp

    for i in range(0,nt):
        
        print(f"i={i}/{nt}, hr={(i*dt[k]/3600)%24:.4f}")
        
        # Run through the CN scheme for interior points
        u_imp = sparse.linalg.spsolve(A_imp,u_imp)
        
        #force to be column vector
        u_imp.shape = (len(u_imp),1)
    
        #update u top boundary
        u_imp[0]=air_temp(i*dt[k])
      
        # Now add the values to their respective lists
        air_temp_list_imp.append(air_temp(i*dt[k]))
        top_ice_temp_list_imp.append(u_imp[0])
        
        #append this array to solution file
        if (i*dt[k])%120 == 0: #every 60 seconds
            u_imp_soln = np.append(u_imp_soln, u_imp, axis=1)
    
    # write the solution matrix to a file
    u_imp_soln = u_imp_soln.transpose()
    np.savetxt(f"im_output_{n+1}_nodes.txt",u_imp_soln, fmt = '%.10f',delimiter=' ')
    implicit_at_3000s_2cm.append(u_imp[40])







#%% Initial and boudnary conditions

cn_at_3000s_2cm = []

for k in range(0, 6):

    nt = int(20/dt[k]); # amount of iterations
    t_days = (dt[k]*nt)/86400.0
    
    # Calculate r, want ~0.25, must be < 0.5 (for explicit method)
    r = ((alpha_ice)*(dt[k]))/(dx*dx);# stability condition
    print("The value of r is ", r)
    
    #%% Set up the scheme

#Create the matrices in the C_N scheme
    #these do not change during the iterations
    
    A_cn = sparse.diags([-r, 1+2*r, -r], [-1, 0, 1], shape = (n+1,n+1),format='lil')
    B_cn = sparse.diags([r, 1-2*r, r], [-1, 0, 1], shape = (n+1,n+1),format='lil')
    
    #now we pad the matrices with one in the TL and BR corners
    A_cn[0,[0,1]] = [1.0,0.0]
    A_cn[1,0] = -r
    A_cn[n,[n-1,n]] = [0.0,1.0]
    A_cn[n-1,n] = -r
    
    B_cn[0,[0,1]] = [1.0,0.0]
    B_cn[1,0] = r
    B_cn[n,[n-1,n]] = [0.0,1.0]
    B_cn[n-1,n] = r
    
    #now convert todifferent format
    
    A_cn = A_cn.tocsc()
    B_cn = B_cn.tocsc()
    
    
    u_cn = T_init(x)
    #set initial BC as well
    u_cn.shape = (len(u_cn),1)
    u_cn[0]=air_temp(0.0)
    u_cn[-1]=273.15

    #initially solve right hand side of matrix equation
    rhs = B_cn.dot(u_cn)
    
    #Create an empty list for outputs and plots
    top_ice_temp_list_cn = []
    air_temp_list_cn = []
    
    #set initial conditions to the matrix as the first row
    u_cn_soln = u_cn
    
    #%% Start Iteration and prepare plots
    
    
    
    
    for i in range(0,nt):
        
        # time in seconds to hours on a 24-hour clock will be used for air temp function
        print(f"i={i}/{nt}, %={(i/nt)*100:.3f}, hr={(i*dt[k]/3600)%24:.4f}")
    
        # Run through the CN scheme for interior points
        u_cn = sparse.linalg.spsolve(A_cn,rhs)
        
        #force to be column vector
        u_cn.shape = (len(u_cn),1)
    
        #update u top boundary
        u_cn[0]=air_temp(i*dt[k])
        
        #update rhs with new interior nodes
        rhs = B_cn.dot(u_cn)
        
        # Now add the surface temp to its list
        top_ice_temp_list_cn.append(u_cn[0])
        
        #append this array to solution file
        if (i*dt[k])%120 == 0: #every 60 seconds
            u_cn_soln = np.append(u_cn_soln, u_cn, axis=1)
        
    # write the solution matrix to a file
    u_cn_soln = u_cn_soln.transpose()
    np.savetxt(f"cn_output_{n+1}_nodes.txt",u_cn_soln, fmt = '%.10f',delimiter=' ')
    cn_at_3000s_2cm.append(u_cn[40])
    


print("exp", explicit_at_3000s_2cm)
print("imp", implicit_at_3000s_2cm)
print("cn", cn_at_3000s_2cm)
print("dt", dt)
print("r", r_list)