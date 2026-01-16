# Class to manage and export to disk data recorded during experiment
#
# Created:
# Nov 20: Naveed Ejaz
import pandas as pd
from datetime import datetime

# Class to manage data generated during experiment
class DataManager:
    sep             = '\t'
    subject_id      = '_'
    nowtime         = ''
    fname           = []    # file name to save data to
    dbg_fname       = []    # file name to save debug data to

    data            = []    # experimental data in pandas format
    dbg_data        = []    # debug data for experiment

    # constructor
    def __init__(self, params):
        self.update_date_string()

        # files for storing experimental and debug data
        self.fname      = self.subject_id + '.txt'
        self.dbg_fname  = self.subject_id + '_debug.txt'

    # get date string
    def update_date_string(self):
        self.nowtime = datetime.now().strftime("%B %d, %Y at %I:%M%p")

    # set subject id (used as file name to save data to disk)
    def set_subject_id(self, subject_id):
        self.subject_id     = subject_id

    # add event to debug data store
    def add_dbg_event(self, event):
        self.update_date_string()
        self.dbg_data.loc[len(self.dbg_data)] = [self.nowtime, self.subject_id, event]

    # add experimental data record to data store
    def add_data_record(self, values):
        self.data.loc[len(self.data)] = values        

    # what is the data, and in which order should datamanager expect it
    def init_data_manager(self, dataformat):
        # initialize experimental data  
        self.data       = pd.DataFrame(columns=dataformat)
        self.dbg_data   = pd.DataFrame(columns=['time_stamp','subject_id','event'])

        # add debug event
        self.add_dbg_event('init')