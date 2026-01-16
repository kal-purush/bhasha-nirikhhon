# -*- coding: utf-8 -*-

import os
import pandas as pd
import collections
import fnmatch
import glob
import os.path as op
from termcolor import colored
import typing
from typing import List, Union

#==============================================================================
#SUBJECT INFOS USING CSV FILE
#==============================================================================

#PATH BASES
#==============================================================================
code_path = os.getcwd()
Analysis_path = op.dirname(op.dirname(code_path))
pathBase = op.dirname(Analysis_path)

#pathBase = '/dycog/meditation/ERC'
#Analysis_path = op.join(pathBase, 'Analyses')
#code_path = op.join(Analysis_path, 'MEG', 'code')

Raw_data_path = op.join(pathBase, 'Raw data')
#==============================================================================


#GET SUBJECT LIST
#==============================================================================
def get_subjlist(task=None, date=None, pathBase = Analysis_path):
     
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)
     
     ld = ld[ld.include == '1']
     if task:
         ld = ld[ld.task == task]
 
     if date:
         ld = ld[ld.date == date]

     ld = ld.id
     try:
         
         subjlist = [ld.iloc[0]]
         
         for k in range(1, ld.shape[0]):
             z = ld.iloc[k]
             if z != ld.iloc[k-1]:
                     subjlist.append(z)
         
         return subjlist
         
     except:
         
         print(colored('No subjects found for this task ...', 'green'))
#==============================================================================


#GET SUBJECT ID
#==============================================================================
def get_id(subj, pathBase = Analysis_path):
     
     if subj.isdigit() == True:
         return subj
         
     if subj.isalpha() == True:
         ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)
             
         ld = ld[ld.name == subj]
         subj_id = ld['id'].iloc[0]
             
         return subj_id
#==============================================================================
    

#GET SUBJECT PROTOCOL
#==============================================================================
def get_protocol(subj, pathBase = Analysis_path):
         
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)
     
     ld = ld[(ld.id == subj) | (ld.name == subj)]    
     
     protocol = ld.iloc[0]['protocol']
         
     return protocol
#==============================================================================








#GET SUBJECT TASK
#==============================================================================
def get_task(subj, block = None, protocol = None, pathBase = Analysis_path):
         
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)
     
     ld = ld[(ld.id == subj) | (ld.name == subj)]
     if protocol:
         ld = ld[ld.protocol == protocol]
     if block:
         ld = ld[ld.block == block]
     
     task = ld.iloc[0]['task']
         
     return task
#==============================================================================
    

#GET SUBJECT DATE
#==============================================================================
def get_date(subj, pathBase = Analysis_path):
          
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)
     
     ld = ld[(ld.id == subj) | (ld.name == subj)]    
     
     date = ld.iloc[0]['date']
         
     return date 
#==============================================================================
 


#GET SUBJECT'S NAME
#==============================================================================
def get_name(subj, pathBase = Analysis_path):
          
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)
     
     ld = ld[(ld.id == subj) | (ld.name == subj)]    
     
     name = ld.iloc[0]['name']
         
     return name
#==============================================================================
     
     
     
     
#GET RAW DATA PATH
#==============================================================================
def get_rawpath(subj, task=None, noise=0, NoiseBase = op.join(Raw_data_path,'MEG_noise'), pathBase = Analysis_path):
        
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)

     if subj.isdigit() == True:   
         ldsub = ld.loc[ld['id'] == subj]
         subj_id = subj
         subj_name = ldsub.iloc[0]['name']
     
     if subj.isalpha() == True:
         ldsub = ld.loc[ld['name'] == subj]
         subj_name = subj
         subj_id = ldsub.iloc[0]['id']
         
     if task:
         ldsub = ldsub[ld.task == task]
         
     protocol = ldsub.iloc[0]['protocol']
     date = ldsub.iloc[0]['date']
     
     if noise==1:
         filename = glob.glob(op.join(NoiseBase, subj_id, subj_name + '_' + protocol + '_' + date + '.misc', 'lyon_Noise_*'))[0]
 
     if noise==0:
         filename =  subj_name + '_' + protocol + '_' + date + '_'
         
     return subj_id, filename
#==============================================================================


#GET AN ORDERED DICT OF A SUBJECT'S BLOCKS AND CORRESPONDING STATES
#==============================================================================
def get_blocks(subj, state=None, task=None, protocol=None, pathBase = Analysis_path):
     
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)

     ld = ld[(ld.id == subj) | (ld.name == subj)]
     
     if state:
         ld = ld[(ld.state == state) | (ld.state == state + '1') | (ld.state == state + '2')]
         
     if task:
         ld = ld[ld.task == task]
         
     if protocol:
         ld = ld[ld.protocol == protocol]
         
     blockstate = dict()
     
     for k in range(ld.shape[0]):
         
         blockstate[ld['block'].iloc[k]] = ld['state'].iloc[k]
         
     odblockstate = collections.OrderedDict(sorted(blockstate.items()))
     
     return odblockstate
#==============================================================================
    

#COUNT THE NUMBER OF SUBJ BLOCKS FOR A GIVEN STATE
#==============================================================================
def state_count(state, pathBase = Analysis_path):
     
     ld = pd.read_table(op.join(pathBase, 'MEG', 'meta', 'listdata.tsv'), dtype=str)
 
     ld = ld[((ld.state == state) | (ld.state == state + '1') | (ld.state == state + '2') | (ld.state == state + '_postFA') | (ld.state == state + '_postOM')) & (ld.include == '1')]
     
     return ld.shape[0]
#==============================================================================



#%% 26/02/2018

# GET CHANNEL NAME
# =============================================================================
#def get_chan_name(subject: typing.Text, chan_type :"'ecg_chan', 'eogH_chan', 'eogV_chan', 'respi_chan', 'egg_chans', 'emgTrap_chan'", data=None) -> Union[typing.Text, List[typing.Text]]:
def get_chan_name(subject, chan_type:"'ecg_chan', 'eogH_chan', 'eogV_chan', 'respi_chan', 'egg_chans', 'emgTrap_chan'", data=None):
    """
    Returns the name of the channel (or a list of channels) of the indicated type in the input dataset, as specified in ../meta/MEG_ANALYSIS_MASTERFILE.csv.
    INPUT
        data: MNE data where to find the channel.
        chan_type: Type of the channel to get the name of.
    """
    master = pd.read_csv('../meta/MEG_ANALYSIS_MASTERFILE.csv', header=3, dtype={'id':str})
    sub_data = master[(master.id == subject)].reset_index(drop=True)
    
    channel = sub_data.get_value(0,chan_type)
    if type(channel) is not str:
        raise ValueError('Channel name not found for this type. Please edit masterfile.')
    
    chans = channel.split('|')
    
    if data:
        data_chans = []
        for ch in chans:
            data_ch = fnmatch.filter(data.ch_names, '*'+ch+'*')
            
            if not data_ch:
                raise ValueError("Channel not found in data.")
            if len(data_ch) > 1:
                warnings.warn("More than one channel found. Check that the masterfile is sufficiently accurate (e.g. 'EEG062' rather than '62').\nUsing channel {}.".format(data_ch[0]))
            
            data_chans.append(data_ch[0])
        
        if len(data_chans) == 1:
            data_chans = data_chans[0]
        return data_chans

    else:
        warnings.warn("No input dataset. Using channel(s) {} as defined in masterfile.".format(chans))
        
        if len(chans) == 1:
            chans = chans[0]
        return chans
# =============================================================================