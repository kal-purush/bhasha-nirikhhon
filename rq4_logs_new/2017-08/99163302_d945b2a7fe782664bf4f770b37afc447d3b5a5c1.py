
# coding: utf-8

# In[119]:

import numpy as np
import math
from sigqc import sigqc_asciitestcase
from sigqc import sigqc_primitives
from sigqc import sigqc_pca as pca
import vibration_toolbox as vtb
import matplotlib
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import decimal
import matplotlib.colors as colors
from sigqc import sigqc_report


# In[125]:

############################
# MassSpringSystem3DOF Class
############################
class MassSpringSystem3DOF:
    '''
    The MassSpringSystem3DOF class is designed to encapsulate all of the information 
    pertaining to an undamped three mass-spring system, including its mass and stiffness matrices,
    diagonalized modal mass and stiffness matrices, resonance frequencies, eigenvectors,
    and mode shapes.
    
    Example
    -------
        m1 = 0.03882
        m2 = 0.005176
        m3 = 0.05952
        
        k1 = 613.0
        k2 = 40.05
        k3 = 601.6
        
        system = MassSpringSystem3DOF()
        system.setMassMatrix(m1, m2, m3)
        system.setStiffnessMatrix(k1, k2, k3)
        K = system.getModalStiffnessMatrix()
        SuperFRF = system.getSuperModalFRFs()
    '''
    def __init__(self):
        '''
        Constructor for an instance of the MassSpringSystem3DOF class.
        '''
        self.m = None
        self.m1 = None
        self.m2 = None
        self.m3 = None
        
        self.k = None
        self.k1 = None
        self.k2 = None
        self.k3 = None
        
        self.M = None
        self.K = None
        self.w = None
        
        self.superfrf = None
        self.freq_range = None
        self._modalcalc = False
        
        # Control significant figures
        np.set_printoptions(suppress=True) 
        
    def setMassMatrix(self, m1, m2, m3):
        '''
        Create mass matrix for three masses to be stored in the 
        MassSpringSystem3DOF object. All three inputs must be floats.
        '''
        self.m = np.zeros((3,3), dtype=float)
        self.m1 = m1
        self.m2 = m2
        self.m3 = m3
        
        self.m[0,0] = m1
        self.m[1,1] = m2
        self.m[2,2] = m3

    def setStiffnessMatrix(self, k1, k2, k3):
        '''
        Create stiffness matrix for three springs to be stored in the 
        MassSpringSystem3DOF object. All three inputs must be floats.
        '''
        self.k = np.zeros((3,3), dtype=float)
        self.k1 = k1
        self.k2 = k2
        self.k3 = k3
        
        self.k[0,0] = (k1+k2+k3)
        self.k[0,1] = -k2
        self.k[0,2] = -k3
        self.k[1,0] = -k2
        self.k[1,1] = k2
        self.k[1,2] = 0.
        self.k[2,0] = -k3
        self.k[2,1] = 0.
        self.k[2,2] = k3
    
    def conductModalAnalysis(self):
        '''
        Conducts modal analysis and sets the following member variables:
        w (list of resonance frequencies), M (diagonalized modal mass
        matrix), and K (diagonalized modal stiffness matrix).
        '''
        # Perform modal analysis...
        self.w, P, S, sinv = vtb.mdof.modes_system_undamped(self.m,self.k) 
        self.M = np.dot(np.dot(sinv,self.m),S)
        self.K = np.dot(np.dot(sinv,self.k),S)
        self._modalcalc = True
        
    def getModalMassMatrix(self):
        '''
        Calls the conductModalAnalysis function if it has not yet been called
        and returns the modal diagonalized mass matrix as a numpy array.
        '''
        if (self._modalcalc):
            return self.M
        else:
            self.conductModalAnalysis()
            return self.M
        
    def getModalStiffnessMatrix(self):
        '''
        Calls the conductModalAnalysis function if it has not yet been called
        and returns the modal diagonalized stiffness matrix as a numpy array.
        '''
        if (self._modalcalc):
            return self.K
        else:
            self.conductModalAnalysis()
            return self.K
    
    def getResonanceFreq(self):
        '''
        Calls the conductModalAnalysis function if it has not yet been called
        and returns the resonance frequences of the system as a numpy array.
        '''
        if(self._modalcalc):
            return self.w
        else:
            self.conductModalAnalysis()
            return self.w
        
    def getSuperModalFRFs(self, freq_range=np.linspace(1,241,num=480)):
        '''
        Calls the conductModalAnalysis function if it has not yet been called
        and returns/stores the superpostion of frequency response functions for 
        the specified input frequency range of the MassSpringSystem3DOF object.

        Inputs
        ------
            freq_range - (Optional) Numeric array-like representing the frequency range
                to use for the FRF. Default is 1 to 240 Hz incremented by 0.5 Hz
 
        Outputs
        -------
            superfrf - Superposition of response functions as a numpy array
        '''
        self.freq_range = freq_range
        if(self._modalcalc):
            # Calculate FRFs of each mode and store in temp. matrix...
            frf = np.zeros((len(self.w), len(freq_range)))
            for nmode in range(len(self.w)):
                for i in range(len(freq_range)):
                    x = freq_range[i]
                    frf[nmode, i] = 1.0 / (np.dot((-x**2),self.M.diagonal()[nmode])+self.K.diagonal()[nmode])
            
            # Superimpose FRFs of each mode to create a single FRF function. Store the magnitude
            # as a member variable...
            self.superfrf = np.sqrt(sum(frf)**2)
        
        else:
            self.conductModalAnalysis()
            # Calculate FRFs of each mode and store in temp. matrix...
            frf = np.zeros((len(self.w), len(freq_range)))
            for nmode in range(len(self.w)):
                for i in range(len(freq_range)):
                    x = freq_range[i]
                    frf[nmode, i] = 1.0 / (np.dot((-x**2),self.M.diagonal()[nmode])+self.K.diagonal()[nmode])
            
            # Superimpose FRFs of each mode to create a single FRF function. Store the magnitude
            # as a member variable...
            self.superfrf = np.sqrt(sum(frf)**2)
        
        return self.superfrf
    
    def compareStiffnessMatrix(self, k1, k2, k3):
        '''
        Returns a numpy array consisting of the ratios of the stiffness
        coefficients of the MassSpringSystem3DOF object to the user-specified
        stiffness coefficients.
        
        Inputs
        ------
        k1 - First stiffness coefficient as numeric type
        k2 - Second stiffness coefficient as numeric type
        k3 - Third stiffness coefficient as numeric type
        
        Outputs
        -------
        ratio - Numpy array of length three describing the ratio of the 
            MassSpringSystem3DOF (experimental) object coefficients to the 
            user-specified stiffness (expected) coefficients 
        '''
        ratio = np.zeros((3))
        
        ratio[0] = (self.k1 - k1)/k1
        ratio[1] = (self.k2 - k2)/k2
        ratio[2] = (self.k3 - k3)/k3
        
        return ratio


# In[126]:

##################################
# MassSpringSystem3DOFGroup class
##################################
class MassSpringSystem3DOFGroup:
    '''    
    The MassSpringSystem3DOFGroup class is designed to encapsulate a group
    of MassSpringSystem3DOF objects for iterative, labeling, and PCA purposes.
    
    Example
    -------
        adjust = np.linspace(0.75, 1.25, num=480)
        k1adjgroup = MassSpringSystem3DOFGroup("K1 Adjusted Only")
        for i in range(len(adjust)):
            # Set up stiffnesses...
            bk1 = adjust[i]*k1
            bk2 = k2
            bk3 = k3

            system = MassSpringSystem3DOF()
            system.setMassMatrix(m1, m2, m3)
            system.setStiffnessMatrix(bk1, bk2, bk3)

            k1adjgroup.addSystem(system)
            
        print(k1adjgroup)
    '''
    def __init__(self, label=None):
        '''
        Constructor for MassSpringSystem3DOFGroup class
        
        Inputs
        ------
            label - (Optional) User-specified name of the group
        '''
        self.group = []
        self.label = label
        self.count = 0
        self.evals = None
        self.evecs = None
        self.pcscores = None
        
    def __str__(self):
        '''
        Overrides the toString method and returns information about the object
        as a string.
        '''
        return str(self.label)+" Group of "+str(self.count)+" MassSpringSystem3DOF objects"
    
    def addSystem(self, systemobj):
        '''
        Appends a MassSpringSystem3DOF object to the group
        '''
        self.group.append(systemobj)
        self.count += 1
    
    def getfrfPCScores(self):
        '''
        Conducts principal component analysis on the entire group of systems' superimposed FRFs
        and returns PC Scores of the system. Stores eigenvalues and eigenvectors within object.
        
        Note: All FRFs of the system MUST be calculated on the same frequency range.
        '''
        if (self.count > 0):
            superfrf1 = self.group[0].getSuperModalFRFs()
            allsuperfrfs = np.zeros((self.count,len(superfrf1)))
            
            for i in range(self.count):
                allsuperfrfs[i] = self.group[i].getSuperModalFRFs()
                
            cov_matrix = pca.getCovariance(allsuperfrfs)
            evals, evecs = pca.getEigen(cov_matrix)
            self.evals, self.evecs = pca.sortEigen(evals, evecs)
            self.pcscores = pca.getPCScores(allsuperfrfs, self.evals, self.evecs)
            print(self.pcscores.shape)
            
            return self.pcscores
        
        else:
            return "Please add MassSpringSystem3DOF objects to calculate PCA on the group..."

    def anglesSumEvecs(self, good_eigvec_set):
        '''
        Returns a list of angles between a summation of test eigenvectors and a summation
        of good eigenvectors. Eigenvectors should/will be calculated from the
        superimposed FRFs of the system.

        Inputs
        ------
            good_eigvec_set - The set of good eigenvectors for comparison with
                test eigenvectors. Should be calculated from the superimposed
                FRF of the system.
        '''
        tot_gevecs = np.sum(good_eigvec_set, axis=0)
        tot_gevecs = tot_gevecs/np.sqrt(sum(tot_gevecs**2))
        angles = []
        for i in range(self.count):
            superfrf = self.group[i].getSuperModalFRFs()
            superfrf = superfrf.reshape(1, len(superfrf))
            tmpcov = np.dot(superfrf.T, superfrf)
            evals, evecs = np.linalg.eigh(tmpcov, UPLO='U')
            tot_evecs = np.sum(evecs,axis=0)
            tot_evecs = tot_evecs/np.sqrt(sum(tot_evecs**2))
            Q = np.dot(tot_gevecs.T, tot_evecs)
            angles.append(math.acos(Q))
        return angles