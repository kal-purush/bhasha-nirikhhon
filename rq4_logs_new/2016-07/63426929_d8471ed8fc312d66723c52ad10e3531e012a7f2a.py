#-- GAUDI jobOptions generated on Fri Jul 22 15:17:24 2016
#-- Contains event types : 
#--   90000000 - 3 files - 71586 events - 5.93 GBytes


#--  Extra information about the data processing phases:


#--  Processing Pass Step-127012 

#--  StepId : 127012 
#--  StepName : Stripping21-Merging-DV-v36r1 
#--  ApplicationName : DaVinci 
#--  ApplicationVersion : v36r1 
#--  OptionFiles : $APPCONFIGOPTS/Merging/DV-Stripping-Merging.py 
#--  DDDB : dddb-20130929-1 
#--  CONDDB : cond-20141107 
#--  ExtraPackages : AppConfig.v3r203;SQLDDDB.v7r10 
#--  Visible : N 

from Gaudi.Configuration import * 
from GaudiConf import IOHelper
IOHelper('ROOT').inputFiles(['LFN:/lhcb/LHCb/Collision12/DIMUON.DST/00041836/0000/00041836_00000340_1.dimuon.dst',
'LFN:/lhcb/LHCb/Collision12/DIMUON.DST/00041836/0000/00041836_00000382_1.dimuon.dst',
'LFN:/lhcb/LHCb/Collision12/DIMUON.DST/00041836/0000/00041836_00000395_1.dimuon.dst'
], clear=True)
FileCatalog().Catalogs += [ 'xmlcatalog_file:dimuonS21.xml' ]