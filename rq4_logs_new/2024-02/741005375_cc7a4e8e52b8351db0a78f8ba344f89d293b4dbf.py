
from graphdiffusion.pipeline import *

import pytest
from torch import nn



def test_example1():
    # Declare the variable as global to modify it
    import os
    os.system("cp -r examples examples_temp_for_testing")
    global TEST_MODUS_WITH_REDUCED_TRAINING
    if "TEST_MODUS_WITH_REDUCED_TRAINING" not in globals():
        TEST_MODUS_WITH_REDUCED_TRAINING = False
        TEST_MODUS_WITH_REDUCED_TRAINING_old = False
    else:
        TEST_MODUS_WITH_REDUCED_TRAINING_old = TEST_MODUS_WITH_REDUCED_TRAINING

    TEST_MODUS_WITH_REDUCED_TRAINING = True
    import examples_temp_for_testing.example1_components
    #import examples_temp_for_testing.example2_spiral
    TEST_MODUS_WITH_REDUCED_TRAINING = TEST_MODUS_WITH_REDUCED_TRAINING_old

    os.system("rm -r examples_temp_for_testing")