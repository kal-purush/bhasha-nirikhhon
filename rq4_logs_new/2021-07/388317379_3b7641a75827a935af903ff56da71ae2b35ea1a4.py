import numpy as np
from scipy import integrate
import matplotlib.pyplot as plt

def airc(t,y): #aircraft
    return np.array([ y[0], -0.313*y[0]+56.7*y[2]+0.232*y[4], y[2], -0.0139*y[0]-0.426*y[2]+0.0203*y[4], y[4], 56.7*y[2] ])


ti,tf= 0,20      # ti e m p o de i n i c i o y f i n de s o l u c i o n
t = np.linspace(ti,tf,100) # e l v e c t o r ti e m p o t con 100 p a s o s
y0=[0,2,1,0,1,3] # c o n d i c i o n e s i n i c i a l 
y = np.zeros((len(t), len(y0))) # d e f i n e e l a r r e g l o de l a s o l u c i o n
y[0,:] = y0
r=integrate.ode(airc).set_integrator("dopri5") # i n t e g r a c i o n
                                                # n um e ri c a 
r.set_initial_value(y0,ti) # c o n f i g u r a l a s c o n d i c i o n e s i n i c i a l e s
for i in range (1, t.size):
    y[i,:]=r.integrate(t[i]) # c o n s e g u i r mas v a l o r e s ,
                                # c o m pl et a e l a r r e g l o
    if not r.successful():
        raise RuntimeError("no_se_consigue_integrar")
plt.plot(t,y)
plt.show()