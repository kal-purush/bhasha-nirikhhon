'''
Created on 18.07.2016

@author: max
'''

from copy import deepcopy

class DiffusionParser():
    
    subOffTag = '}'
    diffusionCoefficients = []
    

    def __init__(self, substances, diffCoeffs):
        self.diffusionCoefficients = deepcopy(substances)
        for i in xrange(len(diffCoeffs)):
            j = 0
            while diffCoeffs[i][j] != self.subOffTag:
                j += 1
            substance = diffCoeffs[i][1:j]
            value = diffCoeffs[i][j+1:]
            if (substance not in self.diffusionCoefficients):
                print("Found diffusion coefficient for substance not in reactions. Aborting.")
                raw_input("Press ENTER to exit program.")
                exit()
            self.diffusionCoefficients[self.diffusionCoefficients.index(substance)] = value
            
            
            
                
            
            
            

        