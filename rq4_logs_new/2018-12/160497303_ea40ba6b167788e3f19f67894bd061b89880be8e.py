# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
# Beginning of the model to determine power requirements for the HAMRAC
import numpy as np
import matplotlib.pyplot as plt
import obj_dumper

density_altitude = lambda h: 1.225*288.16/(15+271.16-1.983*h/304.8)*(1-(0.001981*h/0.3048)/288.16)**5.256
v_climb = lambda vh, Pexcess, Phov: 2/2*vh*Pexcess/Phov*(Pexcess/Phov-2)/(Pexcess/Phov-1)
Pexcess = lambda vh, vc, Phov: Phov*(vc/2/vh-1+np.sqrt((vc/2/vh)**2+1))
determineTipPathPlane = lambda W, v_cl, p_par, v: (W*v_cl+P_par)/v/W

def verify_lambda(vh, Phov):
    '''
    Function to verify the vc and Pexcess lambdas used for climb performance
    
    Parameters
    ----------
    vh : float
        Induced velocity (vi) in hover
    Phov : float
        induced power (Pi) in hover
    
    Returns
    -------
    ys : array_like
        An array of climb rates from sample Pexcess data
    y2 : array_like
        An array of climb rates from Pexcess data calculated from Pexcess lambda
    ys/y2 : array_like
        Ratios of the two climb rate calculations to check for accuracy
    '''
    xs = np.arange(1, 100, 1)
    ys = v_climb(vh, xs, Phov)
    x2 = Pexcess(vh, ys, Phov)
    y2 = v_climb(vh, x2, Phov)
    
    return ys, y2, ys/y2


def extrapolate(x, y):
    '''
    Quadratic extrapolation function for a data set with a level start.
    
    Parameters
    ----------
    x : array_like
        a data set with 'horizontal' data starting from index 0, e.g. [3,3,3,3,3,5,6,7]
    y : array_like
        the x-coordinates associated with pi
        
    Returns
    -------
    nd_array
        an array of n length with (x,y) coordinates for the quadratic extrapolation, where
        n is the length of the horizontal data to be smoothed over

    '''
    dydx = [(y[i+1] - y[i])/(x[i+1]-x[i]) for i in range(len(y)-1)]  # forward rate-of-change
    at_cruise = next((i for i, n in enumerate(dydx) if n), None) + 1  # second non-zero term, dydx of P_i,cr
    d2ydx2 = dydx[at_cruise]/at_cruise
    
    m = dydx[at_cruise]
    v = y[at_cruise]
    spline = [[at_cruise, v]]
    
    for j in range(at_cruise, 0, -1):
        m -= d2ydx2
        u = j-1
        v -= m*(x[j+1]-x[j])
        spline.append([u, v])
        
    return np.array(spline)
    
    
def determineP_to(v_inf, W, rho, R, alpha, sigma, CDp, Omega, f, CDS):
    ''' 
    Determine the total power required of a helicopter in level, forward flight
    
    Parameters
    ----------
    v_inf : array_like
        The airspeed(s) of the helicopter in m/s 
    W : float
        The weight of the helicopter in N        
    rho : float
        The density of the surrounding air in kg/m^3. Recommend ISA data        
    R : float
        Radius of the rotors of the helicoter in m    
    alpha : float
        Angle of TPP (tip-path-plane). Set to 0 for level flight    
    sigma : float
        Solidity of the rotor (blade area / disc area)        
    CDp_bar : array_like, float
        (Average) drag coefficient of the rotor blade. Constant when blade is constant        
    Omega : float
        Angular velocity of the rotor in rad/s        
    f : float
        Equivalent wetted area of the helicopter. Recommend 0.93 for small helicopter, and 4.65 for large helicopter        
    CDS : float
        Equivalent flat plate area of the helicopter. Values range from 1 (aerodynamicly efficient) to 6 (aerodynamically inefficient)        
        
    Returns
    -------
    P_to : array_like
        The total power required for the helicopter at airspeed
    P_i : array_like
        The induced power for the helicopter at airspeed
    P_pd : array_like
        The profile drag power for the helicopter at airspeed
    P_par : array_like
        The parasite drag power for the helicopter at airspeed
    P_c : array_like
        The climb power required for the helicopter at airspeed
    P_tr : array_like
        The tail power required for the helicopter at airspeed
    '''
    myu = v/(Omega*R)
    vi_hov = (np.sqrt(W/(2*rho*np.pi*R*R)))
    vi_cr = vi_hov**2 / np.sqrt((v*np.cos(alpha))**2+(v*np.sin(alpha)+vi_hov)**2)  # from cunha lecture 6 # verified by Max
    
#    pi_hov = W*vi_hov
#    pi_cr = W*vi_cr 
  
#    P_i = np.where(pi_hov<pi_cr, pi_hov, pi_cr)
    P_i = W*vi_cr
    P_pd = sigma*CDp/8 * rho* ((Omega*R)**3) * np.pi*R*R * (1+f*myu**2)
    P_par = CDS*0.5*rho*v_inf**3
    P_c = 0.  # power to climb, set to 0 for current workings
    P_tr = 0.  # tail rotor power, set to 0 for configuration
    
    P_to = P_i + P_pd + P_par + P_c + P_tr  # marilena
    
    return P_to, P_i, P_pd, P_par, P_c, P_tr

def determineV_chars(P_to, v, P_e, eta_m, W):
    '''
    Calculates relevant velocity values of a helicopter from its power requirements
    
    Parameters
    ----------
    P_to : array_like
        Power curve of the helicopter [W]
    v : array_like
        Airspeeds associated with the power curve [m/s]
    P_e : float
        Engine power of the helicopter [W]
    eta_m : float
        Mechanical efficiency
    W : float
        Weight of the helicopter [N]
        
    Returns
    -------
    v_A : float
        Minimum power/fuel flow/maximum climb/minimum descent airspeed [m/s]
    v_B : float
        Maximum specific flight range airspeed, airspeed for minimum descent angle [m/s]
    v_C : float 
        Maximum airspeed, 0 rate of climb airspeed [m/s]
    p_min : float
        Power required at v_A (minimum power) [W]
    P_sfr : float
        Power required at v_A (power required for maximum specific flight range) [W]
    '''
    # transmission losses
    P_a = eta_m * P_e
    
    # minimum power airspeed
    p_min = min(P_to)
    v_A = v[np.where(P_to==p_min)]
    
    # minimum fuel flow airspeed (maximum range airspeed)
    # method: determine slope at all points on graph, determine slope of line between origin and said point, then find which one is the closest
    dydx = [(P_to[i+1]-P_to[i-1])/(v[i+1]-v[i-1]) for i in range(1, len(P_to)-1)]
    m = [P_to[j]/v[j] for j in range(1, len(P_to)-1)]
    difference = [abs((dydx[k]-m[k])/m[k]) for k in range(len(m))]
    closest_fit = min(difference)
    closest_loc = difference.index(closest_fit)
    p_sfr = P_to[closest_loc]
    v_B = v[closest_loc]
    
    # maximum airspeed
    v_C = v[np.where(P_to>P_a)[0][0]]  # does not work when hover is critical
    
    # climb rate
    v_cl = (P_a-P_to)*2/W
    v_cl = np.where(v_cl<0, 0, v_cl)  # cunha 16
    
    return v_A, v_B, v_C, v_cl, p_min, p_sfr

def alpha_calcs(W, P_par, v_cl, v, v_sfr):
    # not working
    alpha_vals = determineTipPathPlane(W, v_cl, P_par, v)
    alpha1 = 10  # arbitrary cutoff point
#    assert len(alpha_vals) > 20, "alpha vals are not more than 20"
#    alpha1 = np.where(alpha_vals==min(alpha_vals[:20]))[0][0]
#    assert alpha1 < 20 and alpha_vals[alpha1-1] > alpha_vals[alpha1] and alpha_vals[alpha1+1] > alpha_vals[alpha1], "minimum alpha index %s is not the local minimum due to grid size"
    zeros = np.zeros(len(alpha_vals))
    
    new_alpha = np.concatenate([zeros[:alpha1], alpha_vals[alpha1:v_sfr+3], zeros[v_sfr+3:]])
    np.savetxt('alpha_vals.dat', new_alpha)
    return new_alpha

# data sets -> put in other file when they grow too large
marilenas_example = {  # used to validate model
    'Nb': 4,  # number of blades
    'k': 1.15, # induced drag power factor, values between 1.1 and 1.2
    'W': 90e3,  # given weight [N]
    'rho': 1.225,  # density of air [kg/m^3]
    'sigma': 0.078,  # solidity of the rotor,
    'CDp_bar': 0.01,  # average drag coefficient (for a homogeneous blade)
    'Omega': 21,  # angular velocity of the rotor [rad/s]
    'R': 9.5,  # radius of the rotor blades
    'c': 0.457,  # chord of a rotor blade
    'v': np.arange(0, 100, 1),
    'f': 4.65,  # equivalent wetted area, Marilena gives 4.65, Cunha gives 4.65 for heavy utility heli and 0.93 for small heli
    'CDS': 3,  # equivalent flat plate area [?] 4 given from Figure 5.11 in Marilena's lecture notes
    'v_tip': 200.,  # tip velocity [m/s], taken from Harrington 2
    'alpha': 0, #np.genfromtxt('alpha_vals.dat'),  # AoA of tip path plane
    'P_e': 2000e3,  # engine power, in [kW]
    'eta_m': 1,
        }

HAMRAC = {
        'Nb': 4,
        'k': 1.15,
        'W': 2500*9.81,
        'rho': density_altitude(8870),  # current number is 7% higher than ISA, from cunha 16
        'sigma': 0.152,  # taken from Harrington 2
        'CDp_bar': 0.01,  # 
        'Omega': 22.3,  # taken from Harrington 2
        'R': 5.38,  # taken from Harrington 2
        'c': 0.645,  # taken from Harrington 2
        'v': np.arange(0, 90, 1),
        'f': 0.93,
        'CDS': 4,
        'v_tip': 120,  # taken from Harrington 2
        'alpha': 0,
        'P_e': 650e3,  # assigned arbitrarily
        'eta_m': 0.95,  # assigned arbitrarily
        }

#newHAMRAC = obj_dumper.make_copy(HAMRAC)
#newHAMRAC['alpha'] = np.genfromtxt('alpha_vals.dat')

data_set = marilenas_example
v = data_set['v']

P_to, P_i, P_pd, P_par, P_c, P_tr = determineP_to(data_set['v'], data_set['W'], data_set['rho'], 
                                                  data_set['R'], data_set['alpha'], data_set['sigma'],
                                                  data_set['CDp_bar'], data_set['Omega'], data_set['f'], data_set['CDS'])

# corrections: P_i2 is induced power with extrapolated data, P_to2 is total power with extrapolated data
# and subscript 3 denotes adjusted power due to equivalent solidity single-rotor approximation (ESSRA)
P_exp = extrapolate(v, P_i)
P_i2 = np.concatenate((P_exp[:,1][::-1], P_i[len(P_exp):]))
P_to2 = P_i2 + P_pd + P_par
adjust = 0.95 # 5% more for coaxial
P_to3, P_i3, P_pd2, P_par2 = P_to2*adjust, P_i2*adjust, P_pd*adjust, P_par*adjust  

v_A, v_B, v_C, v_cl, P_min, P_sfr = determineV_chars(P_to2, v, data_set['P_e'], data_set['eta_m'], data_set['W'])
alphas = alpha_calcs(data_set['W'], P_par, v_cl, v, v_B)

vls = verify_lambda((np.sqrt(2500*9.81/(2*1.225*np.pi*9.5*9.5))), P_to3[0])
plt.figure()
plt.xlim(v[0], v[-1]*1.3)
plt.ylim(0, max(P_to3/1000)*1.1)
plt.grid()
plt.xlabel('airspeed V [m/s]')
plt.ylabel('power P [kW]')

show_extrapolate = False
if show_extrapolate:
    # quadratic spline extrapolation, approximates for tail power included
    plt.title('Power requirements for a helicopter in level forward flight \nwith quadratic extrapolation')
    fname = 'newHAMRAC.png'
    plt.plot(v, P_to3/1000, label='P_to,exp')
    plt.plot(v, P_i3/1000, label='P_i,exp')
else:
    plt.title('Power requirements for a helicopter in level forward flight')
    plt.plot(v, P_to/1000, label='P_to')
    plt.plot(v, P_i/1000, label='P_i')
    fname = 'power_req_HAMRAC.png'
    
plt.plot(v, P_pd2/1000, label='P_pd')
plt.plot(v, P_par2/1000, label='P_par')

plt.axhline(data_set['P_e']*data_set['eta_m']/1000, 0, v_C/max(v)/1.3, label='P_a', color='blue', linestyle='dotted')
plt.axvline(v_C, 0, data_set['P_e']*data_set['eta_m']/max(P_to3)/1.1, label='v_max', color='blue', linestyle='dashed')
plt.plot((0,v_B), (0, P_sfr/1000), color='purple')
plt.axvline(v_B, 0, P_sfr/max(P_to3)/1.1, label='v_sfr', color='purple', linestyle='dashed')
plt.axvline(v_A, 0, P_min/max(P_to3)/1.1, label='v_pmin', color='brown', linestyle='dashed')
plt.axhline(P_min/1000, 0, v_A/max(v)/1.3, color='brown', linestyle='dotted')
plt.legend(loc='lower right')
#plt.savefig(fname)

plt.figure()
plt.plot(v, P_to2, label='unadjusted')
plt.plot(v, P_to3, label='adjusted (5% less)')
plt.title('Difference between adjusted and unadjusted P_to by 5%')
plt.xlabel('airspeed V [m/s]')
plt.ylabel('power P [kW]')
plt.grid()
plt.legend(loc='lower right')
plt.savefig('adjustedPto.png')

