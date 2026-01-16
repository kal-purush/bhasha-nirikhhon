# Initilization of Constants, parameters and distributions.
#import numpy as np

# CONSTANT:
# scalars
FOUNDER_COUNT = 10^5 # Nr. initial agents

SAMPLE_COUNT = 10^5 # Nr. sampled agents for each cycle

YIELD = 5 # Nr. population doublings.

NR_CYCLES = 50

LAG_TIME_MEAN = 90 # Lag phase

CELL_CYCLE_TIME_MEAN = 90

GENOME_LENGTH = 5 # Nr. of possible mutations.

MAXIMUM_NR_AGENTS = YIELD * FOUNDER_COUNT


# vectors
LAG_TIMES = LAG_TIME_MEAN + np.random.normal(0, 1, [FOUNDER_COUNT, 1])

CELL_CYCLE_TIMES = CELL_CYCLE_TIME_MEAN + np.random.normal(0, 1, [FOUNDER_COUNT, 1])

PARENTS = range(FOUNDER_COUNT)

#MUTATION_PROB = data1[range(5)]

#MUTATION_CELL_CYCLE_EFFECT = [0.5, 1.2


# CHANGEABLE:
# vectors
lag_progress = np.zeros([MAXIMUM_NR_AGENTS,1])