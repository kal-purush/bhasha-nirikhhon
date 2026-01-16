from numpy import *
from matplotlib.pyplot import *
from scipy.optimize import fsolve
import time


# Implizite Mittelpunktsregel

def IM(u0, f, N, T, t0=0):
    """Implicit Midpoint Rule

    Input:
    u0:    Initial values u(t0) := [x(t0), x'(t0)]
    f:     Rechte-Seite f(t, u)
    N:     Anzahl Schritte
    T:     Endzeit
    t0:    Anfangszeit

    Output:
    t, u:  Zeit und Loesung
    """
    t, h = linspace(t0, T, N, retstep=True)
    u = zeros((u0.size, N))
    ################################################################
    #                                                              #
    # TODO: Implementieren Sie hier die implizite Mittelpunksregel #
    #                                                              #
    ################################################################
    u[:,0] = u0

    for k in xrange(N-1):
        F = lambda x: x - u[:,k] - h * f(t[k]+0.5*h, 0.5*(x + u[:,k]))
        u[:,k+1] = fsolve(F, u[:,k] + h * f(t[k], u[:,k]))

    return t, u


# Runge-Kutta Butcher scheme for implicit midpoint rule
#bs_A = array([[0.5]])
#bs_b = array([1.0])
#bs_c = array([0.5])


# Runge-Kutta Gauss Collocation of Order 6
bs_A = array([[ 5.0/36.0,               2.0/9-sqrt(15)/15.0, 5.0/36.0-sqrt(15)/30.0],
              [ 5.0/36.0+sqrt(15)/24.0, 2.0/9,               5.0/36.0-sqrt(15)/24.0],
              [ 5.0/36.0+sqrt(15)/30.0, 2.0/9+sqrt(15)/15.0, 5.0/36.0              ]])

bs_b = array([5.0/18.0, 4.0/9.0, 5.0/18.0])

bs_c = array([0.5-sqrt(15)/10, 0.5, 0.5+sqrt(15)/10])


def run_im():
    # Alle benoetigten Parameter sind als globale Variablen verfuegbar
    Ioim = array([0.0, 0.0])
    #######################################################################
    #                                                                     #
    # TODO: Loesen Sie die Gleichung mit der impliziten Mittelpunktsregel #
    #       und obigen Anfangswerten Ioim = [y(t0), y'(t0)]               #
    #                                                                     #
    #######################################################################
    t, y = IM(Ioim, f, nsteps, T)
    return "im", t, y


def run_rk():
    # Alle benoetigten Parameter sind als globale Variablen verfuegbar
    from rk import RungeKutta
    #################################################################
    #                                                               #
    # TODO: Achtung, Klassen-Template in rk.py umbenennen damit der #
    #       obige Import korrekt funktioniert                       #
    #################################################################
    Iork = array([0.0, 0.0])
    ########################################################################
    #                                                                      #
    # TODO: Loesen Sie die Gleichung mit der Runge-Kutta Gauss-Collocation #
    #       und obigen Anfangswerten Iork = [y(t0), y'(t0)]. Benutzen Sie  #
    #       die RungeKutta Klasse: erstellen Sie eine Instanz und rufen    #
    #       Sie die Member-Methoden korrekt auf.                           #
    #                                                                      #
    ########################################################################
    rk = RungeKutta(bs_A, bs_b, bs_c)
    rk.set_rhs(f)
    rk.set_iv(0, Iork)
    t, y = rk.integrate(T, nsteps)
    return "rk", t, y


def run_ode45():
    # Alle benoetigten Parameter sind als globale Variablen verfuegbar
    from ode45 import ode45
    Io45 = array([0.0, 0.0])
    ################################################################
    #                                                              #
    # TODO: Loesen Sie die Gleichung mit der dem ode45 Integrator  #
    #       und obigen Anfangswerten Io45 = [y(t0), y'(t0)]        #
    #                                                              #
    ################################################################
    t, y = ode45(f, array([0, T]), Io45, initialstep=2e-5, reltol=1e-8, abstol=1e-8)
    return "ode45", t, y.T
    
def run_odeint():
    # Alle benoetigten Parameter sind als globale Variablen verfuegbar
    from scipy.integrate import odeint
    Ioint = array([0.0, 0.0])
    ################################################################
    #                                                              #
    # TODO: Loesen Sie die Gleichung mit der dem ode45 Integrator  #
    #       und obigen Anfangswerten Io45 = [y(t0), y'(t0)]        #
    #                                                              #
    ################################################################
    t = linspace(0, T, nsteps)
    y = odeint(lambda y, t: f(t,y), Ioint, t)
    return "odeint", t, y.T




if __name__ == '__main__':
    # Bauteile
    R = 100e3  # 100 KOhm
    C = 10e-9  # 10 nF
    L = 50e-3  # 50 mH

    # Signal
    Vo = 5  # 5 V
    w = 50  # 50 Hz
    Vin = lambda t: Vo * sin(2*pi*w*t)
    ###Vin = lambda t: sin(2*pi*t)
    
    # Diodenmodell
    Is = 1e-9   # 1 nA
    Ut = 25e-3  # 25 mV
    n = 1.0
    Id = lambda t, yp: Is * (exp((Vin(t) - L*yp)/(n*Ut)) - 1.0)

    # Gleichung  f(t, y) = [y'(t), y''(t)]
    f = lambda t, y: array([0.0,
                            0.0])
    #############################################################
    #                                                           #
    # TODO: Implementieren Sie hier die rechte Seite f(t, y(t)) #
    #                                                           #
    #############################################################
    
    f = lambda t, y: array([y[1], 1/L/C*Id(t,y[1])-1/R/C*y[1]-1/L/C*y[0]]) # why is it Id(t,y[0]) instead of Id(t,y[1])?
    ###f = lambda t, y: array([y[1], Vin(t)-y[1]-y[0]])

    # Number of periods
    np = 1
    # Number nodes per period
    nn = 8000
    # Number of steps in total
    nsteps = np*nn + 2*nn/4 + 1
    # Endzeit
    T = np*0.02 + 2*0.005
    #T = 12

    print("Anzahl der Perioden: %d" % np)
    print("Anzahl Zeitschritte: %d" % nsteps)
    print("Endzeit: %f" % T)

    methodname = ""
    t = zeros((nsteps,))
    y = zeros((2,nsteps))
    ############################################################################
    #                                                                          #
    # TODO: Waehlen Sie hier die gewuenschte Methode durch aus/einkommentieren #
    #                                                                          #
    ############################################################################
    methodname, t, y = run_im()
    #methodname, t, y = run_rk()
    #methodname, t, y = run_ode45()
    #methodname, t, y = run_odeint()


    Vout = zeros_like(t)
    IR = zeros_like(t)
    IC = zeros_like(t)
    ID = zeros_like(t)
    IL = zeros_like(t)
    ###################################################
    #                                                 #
    # TODO: Berechnen Sie hier die fehlenden Groessen #
    #                                                 #
    ###################################################
    IL = y[0]
    ID = Id(t, y[1])
    IR = L/R*y[1]
    IC = ID-IR-IL
    Vout = L*y[1]
    ###Vout = y[0]

    figure()
    plot(t, Vin(t), label=r'$V_{in}(t)$')
    plot(t, Vout, label=r'$V_{out}(t)$')
    xlabel(r'$t \; [s]$')
    ylabel(r'$V \; [V]$')
    grid(True)
    ylim(-Vo - 1, Vo + 1)
    legend(loc='lower right')
    savefig('vosc_'+methodname+'.png')

    figure()
    plot(t, Vin(t), label=r'$V_{in}(t)$')
    plot(t, Vout, label=r'$V_{out}(t)$')
    xlabel(r'$t \; [s]$')
    ylabel(r'$V \; [V]$')
    grid(True)
    xlim(0.016, 0.021)
    ylim(-Vo - 1, Vo + 1)
    legend(loc='upper right')
    savefig('vosc_zoom_'+methodname+'.png')

    figure()
    plot(t, squeeze(ID), label=r'$I_D(t)$')
    plot(t, squeeze(IR), label=r'$I_R(t)$')
    plot(t, squeeze(IC), label=r'$I_C(t)$')
    plot(t, squeeze(IL), label=r'$I_L(t)$')
    grid(True)
    xlabel(r'$t \; [s]$')
    ylabel(r'$I \; [A]$')
    legend(loc='upper right')
    savefig('iosc_'+methodname+'.png')

    figure()
    #plot(t, squeeze(ID), label=r'$I_D(t)$')
    plot(t, squeeze(IR), label=r'$I_R(t)$')
    plot(t, squeeze(IC), label=r'$I_C(t)$')
    plot(t, squeeze(IL), label=r'$I_L(t)$')
    grid(True)
    xlim(0.016, 0.021)
    ylim(-0.0025, 0.0025)
    xlabel(r'$t \; [s]$')
    ylabel(r'$I \; [A]$')
    legend(loc='upper right')
    savefig('iosc_zoom_'+methodname+'.png')