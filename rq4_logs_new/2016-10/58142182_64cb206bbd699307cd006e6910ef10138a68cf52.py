__author__ = "Ben Nelsen"

"""
@@ Class level doc string here
"""

class EMD:

    def __init__(self):
        self.x = []                         # Signal to be decomposed
        self.theta1 = 0.05                  # Defines Rilling's variable to determine "Global Smallness"
        self.alpha = 0.5                    # Defines Rilling's variable determining % of time global smallness need not be satisfied
        self.thteta2 = 0.05                 # Allowance for values where global smallness is not satisfied
        self.display = 0                    #
        self.maxiterations = 2000           # Prevents EMD from iterating more than 2000 times.
        self.fix = 0                        #
        self.maxmodes = 0                   #
        self.interpolation = 'spline'       # Default spline method set to spline
        self.fix_h = 0                      #
        self.mask = 0                       #
        self.ndirs = 4                      #
        self.complex_version = 2            #
        self.lengthx = 0
        self.r = []
        self.nbit = 0
        self.NbIt = 0
        self.numExtrema = 0
        self.numZero = 0
        self.t = 0

    def loadAlternateSettings(self, **kwargs):
        for key in kwargs:
            if key == 'STOP':
                self.theta1 = kwargs[key][0]
                self.alpha = kwargs[key][1]
                self.theta2 = kwargs[key][2]
            if key == 'theta1':
                self.theta1 = kwargs[key]
            if key == 'alpha':
                self.alpha = kwargs[key]
            if key == 'theta2':
                self.theta2 = kwargs[key]
            if key == 'display':
                self.display = kwargs[key]
            if key == 'maxiterations':
                self.maxiterations = kwargs[key]
            if key == 'fix':
                self.fix = kwargs[key]
            if key == 'maxmodes':
                self.maxmodes = kwargs[key]
            if key == 'interpolation':
                self.interpolation = kwargs[key]
            if key == 'fix_h':
                self.fix_h = kwargs[key]
            if key == 'mask':
                self.mask = kwargs[key]
            if key == 'ndirs':
                self.ndirs = kwargs[key]
            if key == 'complex_version':
                self.complex_version = kwargs[key]

    def initializeTimeSeriesVariables(self, da_dataSeries):
        self.x = da_dataSeries
        self.lengthx = len(da_dataSeries)
        self.r = da_dataSeries
        self.numExtrema = len(da_dataSeries)
        self.numZero = len(da_dataSeries)
        self.t = range(0,len(da_dataSeries))

    def runEMD(self, da_dataSeries, **alternateSettings):
        self.__init__()
        self.loadAlternateSettings(alternateSettings)
        self.initializeTimeSeriesVariables(da_dataSeries)
        print('debug')



