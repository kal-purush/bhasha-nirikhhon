# ======================================================
# Checkpoint 2  :  code supplied to students at start.
#
# Exact form of Charge Distribution


import math
import matplotlib.pyplot as pl



#---------------------------------------
# Charge distribution at the PN junction
class ChargeDistribution:
    
    #..............................................
    # Methods for the user of this class
    
    # To evaluate the y-value of the charge for an input x-value
    def evaluate(self, x):
        if( x < self.x1): return 0
        if( x < self.x2): return self._shape( self.x1, self.x2, x)
        if( x < self.x3): return -self._shape( self.x3, self.x2, x)
        return 0.
    
    # To plot the function on the screen
    def show(self, title='', disp=True ):
        xvalues, yvalues = self._get()
        pl.plot( xvalues, yvalues )
        pl.title( title )
        if(disp):pl.show()
    
    #...........................................
    
    #constructor
    def __init__(self):
        self.x0 = -2.
        self.x1 = -1.
        self.x2 = 0.
        self.x3 = 1
        self.x4 = 2
        self.k = math.pi/(self.x3-self.x1)
    
    # pseudo internal methods
    def _shape(self, x0, x1, x):
        z = (x-x0)/(x1-x0)
        return (z**2)* (math.exp(1-z)-1.) / 0.18
    
    def _get( self, start=-2, end=2., n=1000 ):
        xvalues= []
        yvalues = []
        dx = (end-start)/n
        for i in range(n):
            xvalues.append(start+i*dx)
            yvalues.append(self.evaluate(start+i*dx))
        return xvalues, yvalues
