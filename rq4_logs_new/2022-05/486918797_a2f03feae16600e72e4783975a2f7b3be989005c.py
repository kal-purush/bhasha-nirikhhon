import numpy as np
import matplotlib.pyplot as plt
import package_DBR
from package_DBR import myRound, SelectPath_RT, Delay_RT, FO_RT, FOPDT, SOPDT, FOPDT_cost, SOPDT_cost, Process, Bode
from IPython.display import display, clear_output

def LeadLag_RT(MV, Kp, Tlead, Tlag, Ts, PV, PVInit=0, method='EBD'):
    
    """
    Help on function LeadLag_RT in module package_DBR Advanced:
        LeadLag RT(MV, Kp, Tlead, Tlag, Ts, PV, PVInit=0, method=" EBD")
        
        The function “LeadLag RT” needs to be included in a ”for or while loop”.
        :MV: input vector
        :Kp: process gain
        :Tlead: lead time constant [s]
        :Tlag: lag time constant [s]
        :Ts:sampling period [s]
        :Pv: output vector
        :PVInit: (optional: default value is 8)
        :method: discretisation method (optional: default value is "EBD')
                EBD: Euler Backward difference
                EFD: Euler Forward difference
                TRAP: Trapezoidal method
        The function appends a value to the output vector "Pv".
        The appended value is obtained from a recurrent equation that depends on the discretisation method.
    """
    #slide 130
    if (Tlag != 0):
        K = Ts/Tlag
        if len(PV) == 0:
            PV.append(PVInit)
        else: # MV[k+1] is MV[-1] and MV[k] is MV[-2]
            if method == 'EBD':
                PV.append((1/(1+K)) * PV[-1] + (Kp*K)/(1+K) * ((1 + (Tlead/Ts)) * MV[-1] - (Tlead/Ts) * MV[-2]))
            elif method == 'EFD':
                PV.append((1-K) * PV[-1] + Kp*K * ((Tlead/Ts) * MV[-1] + (1-Tlead/Ts) * MV[-2]))
            #elif method == 'TRAP':
                #PV.append()            
            else:
                PV.append((1/(1+K)) * PV[-1] + (Kp*K)/(1+K) * ((1 + (Tlead/Ts)) * MV[-1] - (Tlead/Ts) * MV[-2]))
    else:
        PV.append(Kp*MV[-1])

        
#--------------------------------------------------------------

def PID_RT(SP,PV,Man,MVMan,MVFF,Kc,Ti,Td,alpha,Ts,MVMin,MVMax,MV,MVP,MVI,MVD,E,ManFF=False,PVInit=0, method ='EBD_EBD'):
    
    """
    The function "PID_RT" needs to be included in a “for or while loop"
    :SP: (or SetPoint) vector
    :PV: PV (or Process Value) vector
    :Man: Man (or Manual controller mode) vector (True or False)
    :MVMan: MVMan (or Manual value for MV) vector
    :NVFF: NVFF (or Feedforward) vector
    
    :Kc: controller gain
    :Ti: integral time constant [s]
    :Td: derivative time constant [s]
    :alpha: Tfd = alpha*Td where Tfd is the derivative filter time constant [s]
    :Ts: sampling period [s]
    :MVMlin: minimum value for MV (used for saturation and anti wind-up)
    :MVMax: maximum value for MV (used for saturation and anti wind-up)
    :Mv: MV (or Manipulated Value) vector
    :MVP: MVP (or Propotional part of MV) vector
    :MVI: MVE (or Integral part of MV) vector
    :MVD: MVD (or Derivative part of MV) vector
    :E: E (or control Error) vector
    :ManFF: Activated FF in manual mode (optional: default boolean value is False)
    :PVInit: Initial value for PV (optional: default value is @): used if PID_RT is ran first in the squence and no value of PV is available yet.
    :method: discretisation method (optional: default value is "EBD')
        EBD-EBD: EBD for integral action and EBD for derivative action
        EBD-TRAP: EBD for integral action and TRAP for derivative action
        TRAP-EBD: TRAP for integral action and EBD for derivative action
        TRAP-TRAP: TRAP for integral action and TRAP for derivative action
 
    The function “PID_RT” appends new values to the vectors "MV", "MVP", "MVI", and "MVD".
    The appended values are based on the PID algorithm, the controller mode, and feedforward.
    Note that saturation of "MV" within the limits [MVMin MVMax] is implemented with anti wind-up.
    """
    # MV[k+1] is MV[-1] and MV[k] is MV[-2]
    if len(PV)==0 :
            E.append(SP[-1]-PVInit)
    else :
        E.append(SP[-1]-PV[-1])
    if Man[-1] == False :
        #proportional part 
        MVP.append(Kc*E[-1])
        #integrating part
        if len(MVI)==0:
            MVI.append((Kc*Ts/Ti)*E[-1])
        else :
            MVI.append(MVI[-1]+(Kc*Ts/Ti)*E[-1])
       
        #derivating part 
        Tfd = alpha*Td
        
        if len(MVD)==0 and len(E)>1:
            MVD.append((Kc*Td/Tfd+Ts)*(E[-1]-E[-2]))
        elif len(E)== 1 :
            MVD.append(0)
        else :
            MVD.append((Tfd/(Tfd+Ts))*MVD[-1]+(Kc*Td/(Tfd+Ts))*(E[-1]-E[-2]))
        MV.append(MVP[-1]+MVI[-1]+MVD[-1])
    else : 
        MVI.append(0)
        MVP.append(0)
        MVD.append(0)
        if len(MVMan)==0 :
            MV.append(0) 
        else : MV.append(MVMan[-1])