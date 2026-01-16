# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 16:40:32 2021

@author: User
"""

import spikeinterface as si
import spikeinterface.extractors as se
import spikeinterface.toolkit as st
import spikeinterface.sorters as ss
import spikeinterface.comparison as sc
import spikeinterface.widgets as sw
import numpy as np
import glob

#import everything else
import os
import sys
import numpy as np
import neo
import pandas as pd
import h5py
import McsPy
import sys, importlib, os
import McsPy.McsData
import McsPy.McsCMOS
from McsPy import ureg, Q_
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import seaborn as sns

from time import strftime

# main directory of the folder to analyse
filedirec = r"D:\Files_Reutlingen_Jenny\main_191021extra"
# sub directory with the actual data
inputdirectory = r"D:\Files_Reutlingen_Jenny\main_191021extra\_output_Spikesorting_29042021_hCSF_child_20min_spont_spikesorting"

os.chdir(inputdirectory)





layerdic = {'layer1':[], 
            'layer2-3':['L1', 'M1', 'M2', 'M3', 'M15', 'M16', 'N1', 'N2', 'N3',
                        'N4', 'N5', 'N6', 'N7','N8', 'N9', 'N10', 'N11', 'N12', 
                        'N13', 'N14', 'N15', 'N16', 'O1', 'O2', 'O3', 'O4', 'O5', 
                        'O6', 'O7', 'O8', 'O9', 'O10', 'O11', 'O12', 'O13', 'O14', 
                        'O15', 'O16', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7', 
                        'P8', 'P9', 'P10', 'P11', 'P12', 'P13', 'P14', 'P15', 
                        'P16', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 
                        'R10', 'R11', 'R12', 'R13', 'R14', 'R15'],
           'layer4':['K1', 'K2', 'K14', 'K15', 'K16', 'L2', 'L3', 'L4', 'L5', 
                     'L6', 'L7', 'L8', 'L9', 'L10', 'L11', 'L12', 'L13', 'L14', 
                     'L15', 'L16', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 
                     'M11', 'M12', 'M13', 'M14'],
           'layer5-6':['K3', 'K4', 'K5', 'K6', 'K7', 'K8', 'K9', 'K10', 'K11', 
                       'K12', 'K13', 'I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 
                       'I8', 'I9', 'I10', 'I11', 'I12', 'I13', 'I14', 'I15', 
                       'I16', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'H7', 'H8', 
                       'H9', 'H10', 'H11', 'H12', 'H13', 'H14', 'H15', 'H16', 
                      'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9', 'G10', 
                      'G11', 'G12', 'G13', 'G14', 'G15', 'G16','F1', 'F2', 'F3', 
                      'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10', 'F11', 'F12', 
                      'F13', 'F14', 'F15', 'F16','E1', 'E2', 'E3', 'E4', 'E5', 
                      'E6', 'E7', 'E8', 'E9', 'E10', 'E11', 'E12', 'E13', 'E14', 
                      'E15', 'E16','D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 
                      'D8', 'D9', 'D10', 'D11'],
           'whitematter':['D12', 'D13', 'D14', 'D15', 'D16', 'C1', 'C2', 'C3', 
                          'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 
                          'C13', 'C14', 'C15', 'C16', 'B1', 'B2', 'B3', 'B4', 
                          'B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'B11', 'B12', 
                          'B13', 'B14', 'B15', 'B16', 'A2', 'A3', 'A4', 'A5', 
                          'A6', 'A7', 'A8', 'A9','A10', 'A11', 'A12', 'A13', 'A14', 'A15' ]}

# loop to invert
layerdic_invert = {}

for key in layerdic:
    for i in layerdic[key]:
        layerdic_invert[i]=key
     
        
labeldic_invert = {}
for key in labeldic:
    keystring = str(labeldic[key])
    labeldic_invert[keystring] = key
    
'''______________________________FUNCTIONS_________________________________'''




    
def units_to_pandas_DataFrame(sorted_dic, recording_cache, dictkey, layerdic_invert, recordingdate='not given'):
    
    # creates pandas DataFrame only including the unit ids. the order will be confused because of the unit numbering ('1' instead of '001')
    unit_ids = sorted_dic[dictkey].get_unit_ids()
    unitframe = pd.DataFrame(
        sorted_dic[dictkey].get_unit_ids(), 
        columns=['unit_ids']
        )
    
    
    list_not_empty_spiketrains = []
    for i in unit_ids:
        st_len = len(sorted_dic[dictkey].get_unit_spike_train(unit_id=i))
        if st_len > 0:
            list_not_empty_spiketrains.append(i)
    
    
    # calculate as many paramters as possible outside of the loop
    recordings_seconds = recording_cache[dictkey].get_num_frames()/recording_cache[dictkey].get_sampling_frequency()
    
    
    for i in list_not_empty_spiketrains:  
        
        # add channel with maximum amplitude
        unitframe.loc[(unitframe['unit_ids']==i), 'max_channel']=st.postprocessing.get_unit_max_channels(
            recording_cache[dictkey], sorted_dic[dictkey], unit_ids=[i]
            )
    
        # get the channel label as on MCS MEA 256 chips
        max_channel = int(unitframe.loc[(unitframe['unit_ids']==i)]['max_channel'])
        unitframe.loc[(unitframe['unit_ids']==i), 'channellabel']= labeldic[max_channel]
        
        # get the number of spiks per unit
        unitframe.loc[(unitframe['unit_ids']==i), 'n_spikes']=len(sorted_dic[dictkey].get_unit_spike_train(unit_id=i))
        
        # add firing rate
        unitframe.loc[(unitframe['unit_ids']==i), 'firing_rate']= unitframe.loc[(unitframe['unit_ids']==i)]['n_spikes']/recordings_seconds
        
        # add layer
        labelkey = unitframe.loc[(unitframe['unit_ids']==i), 'channellabel'].values[0]
        unitframe.loc[(unitframe['unit_ids']==i), 'layer']= layerdic_invert[labelkey]

        
        unitframe.loc[(unitframe['unit_ids']==i), 'file']=filebase
        unitframe.loc[(unitframe['unit_ids']==i), 'subrecording']=dictkey
        unitframe.loc[(unitframe['unit_ids']==i), 'recordingdate']=recordingdate
        # verify if this line works
        unitframe.loc[(unitframe['unit_ids']==i), 'medium']=unitframe.loc[(unitframe['unit_ids']==i)]['file'].split('_')[0]
    return unitframe





