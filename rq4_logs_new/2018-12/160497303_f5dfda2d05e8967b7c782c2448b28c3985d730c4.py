# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 11:35:24 2018

@author: Matt
"""
#from __future__ import division 
import numpy as np
import matplotlib.pyplot as plt

class DesignSpace(object):
    
    def __init__(self):
        self.v_cr = 140/1.9
        self.Nb = 3

        self.statistical_boundaries = {
            'c': [0.19, 0.45],
            'R': [3.5, 7],
            'Omega': [25, 42],
            'Nb': [3, 4],
            'cbar': [0.04, 0.08],  # c/R
            'A': [12.5, 25],  # R/c
            'sigma': [0.03, 0.12],  # Nb*c/pi/R
            'bl': [0.07, 0.1],  #  W/pi/R/R/rho/vtip/vtip/sigma 
            'vtip': [180, 220],  # Omega * R + vair
            'myu': [0.25, 0.4],  # vair / vtip
        }        
        
    def calca(self,):
        return np.sqrt(self.gamma, self.R, self.get_T(self.h))
        
    
    def get_Rafovtip_Omega(self):
        vtip1, vtip2 = self.statistical_boundaries['vtip']
        Omega1, Omega2 = self.statistical_boundaries['Omega']
       
        R1 = vtip1/Omega1
        R2 = vtip1/Omega2
        R3 = vtip2/Omega2
        R4 = vtip2/Omega1
        
        return R1, R2, R3, R4
        
    def get_Omegaafovtip_R(self):
        vtip1, vtip2 = self.statistical_boundaries['vtip']
        R1, R2 = self.statistical_boundaries['R']
        
        Omega1 = vtip1/R1
        Omega2 = vtip1/R2
        Omega3 = vtip2/R2
        Omega4 = vtip2/R1
        
        return Omega1, Omega2, Omega3, Omega4
        
    def get_Rafomyu_Omega(self):
        myu1, myu2 = self.statistical_boundaries['myu']
        Omega1, Omega2 = self.statistical_boundaries['Omega']
        
        R1 = self.v_cr / Omega1 / myu1
        R2 = self.v_cr / Omega1 / myu2
        R3 = self.v_cr / Omega2 / myu2
        R4 = self.v_cr / Omega2 / myu1
        
        return R1, R2, R3, R4
        
    def get_Omegaafomyu_R(self):
        myu1, myu2 = self.statistical_boundaries['myu']        
        R1, R2 = self.statistical_boundaries['R']
        
        Omega1 = self.v_cr / R1 / myu1
        Omega2 = self.v_cr / R1 / myu2
        Omega3 = self.v_cr / R2 / myu2
        Omega4 = self.v_cr / R2 / myu1
        
        return Omega1, Omega2, Omega3, Omega4
        
    def get_Rafosigma_c(self):
        c1, c2 = self.statistical_boundaries['c']
        sigma1, sigma2 = self.statistical_boundaries['sigma']
        
        R1 = self.Nb*c1/np.pi/sigma1
        R2 = self.Nb*c1/np.pi/sigma2
        R3 = self.Nb*c2/np.pi/sigma2
        R4 = self.Nb*c2/np.pi/sigma1
        
        return R1, R2, R3, R4

    def get_cafosigma_R(self):
        R1, R2 = self.statistical_boundaries['R']
        sigma1, sigma2 = self.statistical_boundaries['sigma']
        
        c1 = R1*np.pi*sigma1/self.Nb
        c2 = R1*np.pi*sigma2/self.Nb
        c3 = R2*np.pi*sigma2/self.Nb
        c4 = R2*np.pi*sigma1/self.Nb

        return c1, c2, c3, c4
        
    def get_RafoA_c(self):
        c1, c2 = self.statistical_boundaries['c']
        A1, A2 = self.statistical_boundaries['A']
        
        R1 = A1*c1
        R2 = A1*c2
        R3 = A2*c2
        R4 = A2*c1
        
        return R1, R2, R3, R4
        
    def get_cafoA_R(self):
        R1, R2 = self.statistical_boundaries['R']
        A1, A2 = self.statistical_boundaries['A']
        
        c1 = R1/A1
        c2 = R1/A2
        c3 = R2/A2
        c4 = R2/A1
        
        return c1, c3, c2, c4  # weird order
        
    def determine_borders(self, Rv, Rm, Omegav, Omegam):
        # v -> vtip, m -> myu 
        # c -> coefficients, p -> polynomial (function), b -> border
        R1, R2 = self.statistical_boundaries['R']
        Omega1, Omega2 = self.statistical_boundaries['Omega']
        xs = np.linspace(R1, R2)

        slopeOv1 = (Omegav[2]-Omegav[1])/(R2-R1)
        yintOv1 = Omegav[1] - slopeOv1*R1
        slopeOv2 = (Omegav[0] - Omegav[3])/(R1-R2)
        yintOv2 = Omegav[3] - slopeOv2*R2
        xsOv1 = np.linspace(Omegav[1], Omegav[2], 50)
        xsOv2 = np.linspace(Omegav[3], Omegav[0], 50)
        Ovb1 = slopeOv1*xsOv1 + yintOv1
        Ovb2 = slopeOv2*xsOv2 + yintOv2
        
        slopeRv1 = (Omega2-Omega1)/(Rv[2]-Rv[1])
        yintRv1 = Omega1 - slopeRv1*Rv[1]
        slopeRv2 = (Omega1-Omega2)/(Rv[0]-Rv[3])
        yintRv2 = Omega1 - slopeRv1*Rv[0]
        xsRv1 = np.linspace(Rv[1], Rv[2], 50)
        xsRv2 = np.linspace(Rv[0], Rv[3], 50)
        Rvb1 = slopeRv1*xsRv1 + yintRv1
        Rvb2 = slopeRv2*xsRv2 + yintRv2

        slopeOm1 = (Omegam[2]-Omegam[1])/(R2-R1)
        yintOm1 = Omegam[1] - slopeOm1*R1
        slopeOm2 = (Omegam[0] - Omegam[3])/(R1-R2)
        yintOm2 = Omegam[3] - slopeOm2*R2
        xsOm1 = np.linspace(Omegam[1], Omegam[2])
        xsOm2 = np.linspace(Omegam[3], Omegam[0])
        Omb1 = slopeOm1*xsOm1 + yintOm1
        Omb2 = slopeOm2*xsOm2 + yintOm2
        
        slopeRm1 = (Omega2-Omega1)/(Rm[2]-Rm[1])
        yintRm1 = Omega1 - slopeRm1*Rm[1]
        slopeRm2 = (Omega1-Omega2)/(Rm[0]-Rm[3])
        yintRm2 = Omega1 - slopeRm1*Rm[0]
        xsRm1 = np.linspace(Rm[2], Rm[1], 50)
        xsRm2 = np.linspace(Rm[3], Rm[0], 50)
        Rmb1 = slopeRm1*xsRm1 + yintRm1
        Rmb2 = slopeRm2*xsRm2 + yintRm2

#        Wack np method
#        Ovc1 = np.polyfit((R1, R2), (Omegav[1], Omegav[2]), 1)        
#        Ovc2 = np.polyfit((R2, R1), (Omegav[3], Omegav[0]), 1)
#        Ovp1 = np.poly1d(Ovc1)
#        Ovp2 = np.poly1d(Ovc2)
#        Ovb1 = Ovp1(xs)
#        Ovb2 = Ovp2(xs)
#        
#        Rvc1 = np.polyfit((Omega1, Omega2), (Rv[1], Rv[2]), 1)
#        Rvc2 = np.polyfit((Omega2, Omega1), (Rv[3], Rv[0]), 1)
#        Rvp1 = np.poly1d(Rvc1)
#        Rvp2 = np.poly1d(Rvc2)
#        Rvb1 = Rvp1(xs)
#        Rvb2 = Rvp2(xs)
#
#        Omc1 = np.polyfit((R1, R2), (Omegam[1], Omegam[2]), 1)        
#        Omc2 = np.polyfit((R2, R1), (Omegam[3], Omegam[0]), 1)
#        Omp1 = np.poly1d(Omc1)
#        Omp2 = np.poly1d(Omc2)
#        Omb1 = Omp1(xs)
#        Omb2 = Omp2(xs)
#        
#        Rmc1 = np.polyfit((Omega1, Omega2), (Rm[1], Rm[2]), 1)
#        Rmc2 = np.polyfit((Omega2, Omega1), (Rm[3], Rm[0]), 1)
#        Rmp1 = np.poly1d(Rmc1)
#        Rmp2 = np.poly1d(Rmc2)
#        Rmb1 = Rmp1(xs)
#        Rmb2 = Rmp2(xs)

        return ((xsOv1, xsOv2, xsRv1, xsRv2, xsOm1, xsOm2, xsRm1, xsRm2), 
                (Ovb1, Ovb2, Rvb1, Rvb2, Omb1, Omb2, Rmb1, Rmb2))
        
    def compare_statistics(self):
        Rmin, Rmax = self.statistical_boundaries['R']
        Omegamin, Omegamax = self.statistical_boundaries['Omega']
        cmin, cmax = self.statistical_boundaries['c']
        
        # R/Omega plot
        Rv1, Rv2, Rv3, Rv4 = self.get_Rafovtip_Omega()
        Omegav1, Omegav2, Omegav3, Omegav4 = self.get_Omegaafovtip_R()
        Rm1, Rm2, Rm3, Rm4 = self.get_Rafomyu_Omega()
        Omegam1, Omegam2, Omegam3, Omegam4 = self.get_Omegaafomyu_R()
        
        # R/c plot
        Rs1, Rs2, Rs3, Rs4 = self.get_Rafosigma_c()
        cs1, cs2, cs3, cs4 = self.get_cafosigma_R()
        RA1, RA2, RA3, RA4 = self.get_RafoA_c()
        cA1, cA2, cA3, cA4 = self.get_cafoA_R()
        
        fig = plt.figure()
#        axRO = fig.add_subplot(111, projection='3d')
        axRO = fig.add_subplot(111)
        plt.title("Design Space for the HAMRAC")
        plt.grid(which='major')
        plt.xlabel('R [m]')
        plt.ylabel('Omega [rad/s]')
#        plt.xlim(3, 8.5)
#        plt.ylim(22, 50)
        
        axRO.plot((Rmin, Rmax, Rmax, Rmin, Rmin), (Omegamin, Omegamin, Omegamax, Omegamax, Omegamin), label='R, Omega constraints')
        axRO.plot((Rv1, Rv2, Rv3, Rv4, Rv1), (Omegamin, Omegamin, Omegamax, Omegamax, Omegamin), label='vtip, R constraints')  
        axRO.plot((Rmin, Rmin, Rmax, Rmax, Rmin), (Omegav1, Omegav2, Omegav3, Omegav4, Omegav1), label='vtip, Omega constraints')        
        axRO.plot((Rm1, Rm2, Rm3, Rm4, Rm1), (Omegamin, Omegamin, Omegamax, Omegamax, Omegamin), label='myu, R constraints')  
        axRO.plot((Rmin, Rmin, Rmax, Rmax, Rmin), (Omegam1, Omegam2, Omegam3, Omegam4, Omegam1), label='myu, Omega constraints')        
        
        plt.legend()
        plt.show()
        
        fig = plt.figure()
        axRc = fig.add_subplot(111)#, projection='3d')
        plt.title("Design Space for the HAMRAC")
        plt.grid(which='major')
        plt.xlabel('R [m]')
        plt.ylabel('c [m]')
#        plt.xlim(3, 10)
#        plt.ylim(0.1, 0.5)

        axRc.plot((Rmin, Rmax, Rmax, Rmin, Rmin), (cmin, cmin, cmax, cmax, cmin))
        axRc.plot((Rs1, Rs2, Rs3, Rs4, Rs1), (cmin, cmax, cmax, cmin, cmin), label='sigma, R constraints')
        axRc.plot((Rmin, Rmax, Rmax, Rmin, Rmin), (cs1, cs2, cs3, cs4, cs1), label='sigma, c constraints')        
        axRc.plot((RA1, RA2, RA3, RA4, RA1), (cmin, cmax, cmax, cmin, cmin), label='A, R constraints')
        axRc.plot((Rmin, Rmax, Rmax, Rmin, Rmin), (cA1, cA2, cA3, cA4, cA1), label='A, c constraints')        
        
        # find borders of the (current) design space with a (series of) np.where calls -> where above/below lines
        # use these borders as inputs into the blade loading limit twice, once for each graph (x no of limits)
        plt.legend(loc='lower right')
        plt.show()
        
        return (((Rv1, Rv2, Rv3, Rv4), (Omegav1, Omegav2, Omegav3, Omegav4)),
                ((Rm1, Rm2, Rm3, Rm4), (Omegam1, Omegam2, Omegam3, Omegam4)),
                ((Rs1, Rs2, Rs3, Rs4), (cs1, cs2, cs3, cs4)),
                ((RA1, RA2, RA3, RA4), (cA1, cA2, cA3, cA4)),
                )
        
    def get_available_space(self, coordsX, coordsY, xss, RVB):
        # rip python usage
        xsOv1, xsOv2, xsRv1, xsRv2, xsOm1, xsOm2, xsRm1, xsRm2 = xss
        Ovb1, Ovb2, Rvb1, Rvb2, Omb1, Omb2, Rmb1, Rmb2 = RVB
        R1, R2 = self.statistical_boundaries['R']
        O1, O2 = self.statistical_boundaries['Omega']
        feasible_cells = []
        for row in range(len(coordsX)):
            for col in range(len(coordsY)):
                R, O = coordsX[row], coordsY[col]
                
                # Om2 is wrong, but also not particularly needed. Rm2, Rv2 shouldn't be needed
                if (R > xsRv1[col] and R < xsRv2[col] and R > xsRm1[col] and R < xsRm2[col]):
                    if O > xsOv1[row] and O < xsOv2[row] and O > xsOm1[row]:# and O < xsOm2[row]:
                        feasible_cells.append([row, col])
        
        return feasible_cells
                    
        
HAMRspace = DesignSpace()
data = HAMRspace.compare_statistics()
data0 = data[0]
R1, R2 = HAMRspace.statistical_boundaries['R']
Omega1, Omega2 = HAMRspace.statistical_boundaries['Omega']
coordsX = np.linspace(HAMRspace.statistical_boundaries['R'][0], HAMRspace.statistical_boundaries['R'][1])
coordsY = np.linspace(HAMRspace.statistical_boundaries['Omega'][0], HAMRspace.statistical_boundaries['Omega'][1])
xss, RVB = HAMRspace.determine_borders(data[0][0], data[1][0], data[0][1], data[1][0])
xsOv1, xsOv2, xsRv1, xsRv2, xsOm1, xsOm2, xsRm1, xsRm2 = xss
Ovb1, Ovb2, Rvb1, Rvb2, Omb1, Omb2, Rmb1, Rmb2 = RVB
#plt.plot(RVB[2], Os, RVB[3], Os, Rs, RVB[0], Rs, RVB[1])

# for each R, Omega point, check its allowable R position, and Omega position
dspace = HAMRspace.get_available_space(coordsX, coordsY, xss, RVB)
dspace = np.array(dspace)
Ropt = coordsX[dspace[:,0]]
Oopt = coordsY[dspace[:,1]]

plt.scatter(Ropt, Oopt)