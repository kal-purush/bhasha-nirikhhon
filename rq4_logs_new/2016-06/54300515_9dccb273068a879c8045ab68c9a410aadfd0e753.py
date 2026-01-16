__author__ = 'Michael Wagner'
__version__ = '1.0'

from cab.cab_global_constants import GlobalConstants


class GC(GlobalConstants):
    def __init__(self):
        super().__init__()
        self.VERSION = '03-2016'
        self.TITLE = 'Foraging Ant'
        ################################
        #     SIMULATION CONSTANTS     #
        ################################
        self.RUN_SIMULATION = False
        self.TIME_STEP = 0
        self.ONE_AGENT_PER_CELL = False
        ################################
        #        ABM CONSTANTS         #
        ################################
        
        ################################
        #         CA CONSTANTS         #
        ################################
        self.USE_HEX_CA = True
        self.USE_CA_BORDERS = True
        self.DIM_X = 75  # How many cells is the ca wide?
        self.DIM_Y = 75  # How many cells is the ca high?
        self.CELL_SIZE = 7
        self.GRID_WIDTH = self.DIM_X * self.CELL_SIZE
        self.GRID_HEIGHT = self.DIM_Y * self.CELL_SIZE
        ################################
        #  SIMULATION SPEC. CONSTANTS  #
        ################################
        self.MAX_ANTS = 25
        self.MAX_PHEROMONE = 100
        self.MAX_FOOD = 100
        self.DIFFUSION = 0.001
        self.EVAPORATION = 0.001