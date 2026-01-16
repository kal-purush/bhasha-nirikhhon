# Main PyPotamus Class
#
# Created:
# Nov 24: Naveed Ejaz
import yaml
import pdb
from psychopy import visual, core

# Load PyPotamus libraries
from ExperimentDisplay import ExperimentDisplay
from TextDisplay import TextDisplay
from Timer import Timer

class Experiment:
    # constructor
    def __init__(self, path):
        self.gParams = yaml.load(open(path + 'defaults.yaml'))
        self.gScreen = ExperimentDisplay(self.gParams)
        self.gText   = TextDisplay(self.gParams)
        self.gTimer  = Timer(self.gParams)

    # reset the handle when the object is destroyed
    def __del__(self):
        self.gParams    = []        
        self.gScreen    = []
        self.gText      = []
        self.gTimer     = []
