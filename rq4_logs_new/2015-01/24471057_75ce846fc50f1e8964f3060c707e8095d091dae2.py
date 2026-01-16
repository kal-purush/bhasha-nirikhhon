import numpy as np

m_s = 50 #mass of rocket shell in kg
g = 9.81 #m/s**2
rho = 1.091 #average air density (assumed constant)
r = 0.5 #in meters
A = np.pi*r**2 #max cross sectional area of the rocket
v_e = 325 #exhaust speed in m/s
c_d = 0.15 #drag coefficient
m_p0 = 100 #initial mass of rocket propellant in kg

def m_p(t):
	"""
	returns the mass of the rocket at time t
	"""
	return m_p0 - np.min([t,5])*20
	
def dm_p(t):
	"""
	returns the derivative of the mass of the rocket w.r.t. time
	"""
	return (t <= 5)*20

def du(t,u):
	"""
	u = [h,v] is a numpy array of length 2
	t is a scalar
	
	m_p = mass of propellant at time t
	dm_p = 20 kg/s for 0 <= t <= 5 and 0 thereafter
	m_p = m_p0 - np.min(t, 5)*20
	"""
	
	u_p = np.zeros(2)
	u_p[0] = u[1]
	u_p[1] = -g + 1/(m_s + m_p(t))*(dm_p(t)*v_e - 0.5*rho*u[1]*np.abs(u[1])*A*c_d)
	
	return u_p
	
def euler_mid(f, t0, dt, N, u0):
	"""modified to use midpoint rule euler's method
	 N = number of time values
	"""
	t = np.linspace(t0, t0+(N-1)*dt, N)
	u = np.zeros((N,2))
	u[0,:] = u0
	 
	for j in range(N-1):
		umid = u[j,:] + (dt/2)*f(t[j], u[j,:])
		tmid = t[j] + dt/2
		u[j+1,:] = u[j,:] + dt*f(tmid, umid) 
	 
	return np.vstack([t,u.T]).T	

def euler(f, t0, dt, N, u0):
	"""
	Euler's method for computing position of rocket at time t
	"""
	t = np.linspace(t0, t0+(N-1)*dt, N)
	u = np.zeros((N,2))
	u[0,:] = u0
	 
	for j in range(N-1):
		u[j+1,:] = u[j,:] + dt*f(t[j], u[j,:]) 
	 
	return np.vstack([t,u.T]).T	

def main():
	dt = 0.01
	t0 = 0
	u0 = np.zeros(2)
	N = 3000
	
	y = euler_mid(du, t0, dt, N, u0)
#	print("t    h    v\n")
#	for j in range(y.shape[0]):
#		print("%3.3f   %4.4f    %4.4f" % (y[j,0], y[j,1], y[j,2]))

	print("%4.4f" % m_p(3.2))
	print(y[:,1].max())
	print(y[:,2].max())
	
	y = euler(du, t0, dt, N, u0)
	print("%4.4f" % m_p(3.2))
	print(y[:,1].max())
	print(y[:,2].max())
	
	

if __name__ == "__main__":
	main()		