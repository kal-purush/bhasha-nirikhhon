# -*- coding: utf-8 -*-
"""
Example of how to run galcluster

@author: Nikolaj Bjerregaard Sillassen
"""
import matplotlib.pyplot as plt
import galcluster as gc

plt.close('all') # Close all plt figures
data_path = "../../Data/" # Define path of data
dataset = "COSMOS_VLA_radio_selected_z_updated2.fits" # Define dataset name
if __name__ == "__main__":
    # Running file as a function
    output = gc.galcluster(data_path+dataset,RAKey="ra",DECKey="dec",method="sigma",sigmas=[5,10],SNlimit=[4,7],min_c_size=3,zKey="z_phot")
    # Running file as an interactive program
    #output = gc.galcluster()