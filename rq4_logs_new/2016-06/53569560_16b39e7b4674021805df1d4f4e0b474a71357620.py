# Basic Equation for Region2
# (p,t)-->volreg2, energyreg2,entropyreg2,enthalpyreg2
#                 cpreg2, cvreg2,spsoundreg2
#------------------------------------------------------------------------
import math

rgas_water=0.461526     # gas constant  KJ/(kg K)
tc_water=647.096        # critical temperature  K
pc_water=22.064         # critical pressure Mpa
dc_water=322.0          # critical density kg/m**3

# Table 10. Page 13,9 I,J,N
I1=0
J1=1
N1=2
I1J1N1=[ [0,0,-96927686500217E+01]
    ,[1,1,0.10086655968018E+02]
    ,[2,-5,-0.56087911283020E-02]
    ,[3,-4,0.71452738081455E-01]
    ,[4,-3,-0.40710498223928E+00]
    ,[5,-2,0.14240819171444E+01]
    ,[6,-1,-0.43839511319450E+01]
    ,[7,2,-0.28408632460772E+00]
    ,[8,3,0.21268463753307E-01]
    ]
def gammareg2(pi,tau):
    """ Fundamental equation for region 2 """
    reg2=0
    for k in range(9):
     reg2+=IJN[k][N1]*math.pow(tau,IJN[k1][J1])   
     return math.log(pi)+reg2

def gammapireg2(pi,tau):
     return 1/pi
 
def gammapipireg2(pi,tau):
    return -1/pi/pi

def gammataureg2(pi,tau):  
    reg2=0
    for k in range(9):
        reg2+=IJN[k][N1]*IJN[k][J1]*math.pow(tau,IJN[k][J1]-1)
        return reg2
    
def gammatautaureg2(pi,tau):  
    reg2=0
    for k in range(9):
        reg2+=IJN[k][N1]*IJN[k][J1]*(IJN[k][J1]-1)*math.pow(tau,IJN[k][J1]-2)
        return reg2
def gammapitaureg2(pi,tau):
    return 0
I2=0
J2=0
N2=0
I2J2N2=[[1,0,-0.17731742473213E-02]
    ,[1,1,-0.17834862292358E-01]
    ,[1,2,-0.45996013696365E-01]
    ,[1,3,-0.57581259083432E-01]
    ,[1,6,-0.50325278727930E-01]
    ,[2,1,-0.33032641670203E-04]
    ,[2,2,-0.18948987516315E-03]
    ,[2,4,-0.39392777243355E-02]
    ,[2,7,-0.43797295650573E-01]
    ,[2,36,-0.26674547914087E-04]
    ,[3,0,0.20481737692309E-07]
    ,[3,1,0.43870667284435E-06]
    ,[3,3,-0.32277677238570E-04]
    ,[3,6,-0.15033924542148E-02]
    ,[3,35,-0.40668253562649E-01]
    ,[4,1,-0.78847309559367E-09]
    ,[4,2,0.12790717852285E-07]
    ,[4,3,0.48225372718507E-06]
    ,[5,7,0.22922076337661E-05]
    ,[6,3,-0.16714766451061E-10]
    ,[6,16,-0.21171472321355E-02]
    ,[6,35,-0.23895741934104E+02]
    ,[7,0,-0.59059564324270E-17]
    ,[7,11,-0.12621808899101E-05]
    ,[7,25,-0.38946842435739E-01]
    ,[8,8,0.11256211360459E-10]
    ,[8,36,-0.82311340897998E+01]
    ,[9,13,0.19809712802088E-07]
    ,[10,4,0.10406965210174E-18]
    ,[10,10,-0.10234747095929E-12]
    ,[10,14,-0.10018179379511E-08]
    ,[16,29,-0.80882908646985E-10]
    ,[16,50,0.10693031879409E+00]
    ,[18,57,-0.33662250574171E+00]
    ,[20,20,0.89185845355421E-24]
    ,[20,35,0.30629316876232E-12]
    ,[20,48,-0.42002467698208E-05]
    ,[21,21,-0.59056029685639E-25]
    ,[22,53,0.37826947613457E-05]
    ,[23,39,-0.12768608934681E-14]
    ,[24,26,0.73087610595061E-28]
    ,[24,40,0.55414715350778E-16]
    ,[24,58,-0.94369707241210E-06]
    ]
#define six parameters to use
def gammarreg2(pi,tau):
    tau=tau-0.5
    reg2=0
    for k in range(43):
        reg2+=I2J2N2[k][N2]*math.pow(pi, I2J2N2[k][I2])*math.pow(tau,I2J2N2[k][J2])
    return reg2
                                          
def gammarpireg2(pi,tau): 
     tau=tau-0.5
     reg2=0
     for k in range(43):
         reg2+=I2J2N2[k][N2]*I2J2N2[k][I2]*math.pow(pi, I2J2N2[k][I2]-1)* math.pow(tau,I2J2N2[k][J2])
     return reg2
 
def gammarpipireg2(pi,tau):  
     tau=tau-0.5
     reg2=0
     for k in range(43):
         reg2+=I2J2N2[k][N2]*I2J2N2[k][I2]*(I2J2N2[k][I2]-1)* math.pow(pi, I2J2N2[k][I2]-2)* math.pow(tau,I2J2N2[k][J2]) 
     return reg2
 
def gammartaureg2(pi,tau):  
     tau=tau-0.5
     reg2=0
     for k in range(43): 
        reg2+=I2J2N2[k][N2]*math.pow(pi,I2J2N2[k][I2])*I2J2N2[k][J2]*math.pow(tau,I2J2N2[k][J2]-1)
     return reg2
 
def gammartautaureg2(pi,tau):
    tau=tau-0.5
    reg2=0
    for k in range(43):  
     reg2+=I2J2N2[k][N2]*math.pow(pi,I2J2N2[k][I2])*I2J2N2[k][J2]*(I2J2N2[k][I2]-1)*math.pow(tau,I2J2N2[k][J2]-2)
     return reg2
 
def gammarpitautaureg2(pi,tau): 
    tau=tau-0.5
    reg2=0
    for k in range(43): 
        reg2+=I2J2N2[k][N2]*I2J2N2[k][I2]* math.pow(pi, I2J2N2[k][I2]-1)* math.pow(tau,I2J2N2[k][J2]-1)  
    return reg2
  
#  p,T -> *
def volreg2(p,T): 
     tau=540/T 
     pi=p/1
     return 0.001*rgas_water*T*pi*(gammapireg2(pi,tau)+gammarpireg2(pi,tau))/p 
 
def energyreg2(p,T):
     tau=540/T 
     pi=p/1
     x=tau*(gammataureg2(pi,tau)+gammartaureg2(pi,tau))-pi*(gammapireg2(pi,tau)+gammarpireg2(pi,tau))
     return x*rgas_water*T

def entropyreg2(p,T): 
     tau=540/T 
     pi=p/1
     y=tau*(gammataureg2(pi,tau)+gammartaureg2(pi,tau))-(gammareg2(pi,tau)+gammarreg2(pi,tau)) 
     return y*rgas_water
     
def enthalpyreg2(p,T):
     tau=540/T 
     pi=p/1
     x=tau*(gammataureg2(pi,tau)+gammartaureg2(pi,tau))
     return x*rgas_water*T
 
def cpreg2(p,T):
     tau=540/T 
     pi=p/1 
     x=-tau*tau*(gammatautaureg2(pi,tau)+gammartautaureg2(pi,tau))
     return x*rgas_water
     
def cvreg2(p,T):
     tau=540/T 
     pi=p/1 
     x=-tau*rau*(gammatautaureg2(pi,tau)+gammartautaureg2(pi,tau))
     y=(-(1+pi*gammarpireg2(pi,tau)-tau*pi*gammarpireg2(pi,tau))**2)/(1-pi*pi*gammarpipireg2(pi,tau))  
     z=x+y
     return z*rgas_water 
def spsoundreg2(p,T):
     tau=540/T 
     pi=p/1 
     a=1+2*pi*gammarpireg2(pi,tau)+pi*pi*gammarpireg2(pi,tau)*gammarpireg2(pi,tau)
     b=1-pi*pi*gammarpipireg2(pi,tau)
     d=tau*tau*(gammatautaureg2(pi,tau)+gammartautaureg2(pi,tau))
     c=((1+pi*gammarpireg2(pi,tau)-tau*pi*gammarpitaureg2(pi,tau))**2)/d
     e=a/(b+c)
     return e*rgas_water*T
 
    
     
     
                                    