# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 16:28:06 2016

@author: galileo
"""

import numpy as np
import scipy

class extraMetrics():
    
    
    def __init__(self):
        
        self.citations = []
        
        
    def findAllMetrics(self):
        
        bd, br = self.bradfordEstimator()
        h = self.hindex()
        pcs = self.percentiles()
        
        # NB only returns the median
        print('all metrics', bd + br + [h] + [pcs[2]]) 
        return bd + br + [h] + [pcs[2]]
            

    def bradfordEstimator(self):
        
        ''' find the zone boundaries fitting Bradford's law '''
        
        # put values in order (smallest to largest)
        citations = np.sort(self.citations)
        totalCites = np.sum(citations)
        
        # find first zone: number of articles accounting for a third of the citations
        n = -1
        cites = -1
        while cites<totalCites/3:
    
            n = n+1        
            cites = cites + self.citations[n]

        # record values            
        bradfordLim1 = n
#        citationLim1 = cites
            
        # find second zone: number of articles accounting for second third of citations
        while cites < 2*totalCites/3:
            
            n = n+1
            cites = cites + self.citations[n]
    
        # record values
        bradfordLim2 = n
#        citationLim2 = cites
        
        # find the 'bradford multiplier', n: ratio of bddry values shoudl be approximately 1:n:n**2
        length = float(len(self.citations))
        try:
            ratios = [float(bradfordLim2)/float(bradfordLim1), length/float(bradfordLim2)]
        except ZeroDivisionError:
            ratios = [0,0]
        
    #    print(bradfordLim1, bradfordLim2)
    #    print(citationLim1, citationLim2, totalCites)
    #    print(ratios)
        
    #    bradfordPc = [float(bradfordLim1)/length * 100., float(bradfordLim2)/length * 100.]
    #    print(bradfordPc)
        
        return [bradfordLim1, bradfordLim2], ratios
        
        
    def hindex(self):
        
        ''' find the h index of a set of citations '''
        
        # arrange in order (largest to smallest)
        citations = np.sort(self.citations)
        citations = citations[::-1]
        
        # go through to the find h index
        n=0
        while citations[n]>=n:
            n = n+1
          
        # return the correct value
        n = n-1
        return n
        
    def percentiles(self):
        ''' find and return some percentiles: 10, 25, 50, 75, 90 '''
        
        pcs = []
        # for a set of percentile values
        for pc in [10, 25,  50, 75, 90]:
            
            # find percentile
            pcs.append(np.percentile(self.citations, pc))
            
        return pcs
        
    
    
if __name__=='__main__':
    
    cites = np.random.randint(0, 100, size=200)
    print(cites)
    
    
#    bradfordEstimator(cites)
#    hindex(cites)
    
    pcs = percentiles(cites)
    
    print(pcs)
    