#-- GAUDI jobOptions generated on Mon Jul 18 13:54:53 2016
#-- Contains event types : 
#--   10010037 - 22 files - 120250 events - 28.87 GBytes


#--  Extra information about the data processing phases:


#--  Processing Pass Step-129442 

#--  StepId : 129442 
#--  StepName : Stripping21r0p1Filtered MD for SLWG (Braun BsKmunu) 
#--  ApplicationName : DaVinci 
#--  ApplicationVersion : v39r1 
#--  OptionFiles : $APPCONFIGOPTS/DaVinci/DV-RedoCaloPID-Stripping21.py;$SEMILEPCONFIGOPTS/Filter_b2KMu.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py 
#--  DDDB : fromPreviousStep 
#--  CONDDB : sim-20141210-1-vc-md100 
#--  ExtraPackages : AppConfig.v3r266;SemilepConfig.v1r11 
#--  Visible : Y 


#--  Processing Pass Step-129249 

#--  StepId : 129249 
#--  StepName : Merge for Stripping21r0p1 SLWG Filtered Productions (Braun)  
#--  ApplicationName : DaVinci 
#--  ApplicationVersion : v39r1 
#--  OptionFiles : $APPCONFIGOPTS/Merging/DVMergeDST.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;$APPCONFIGOPTS/Merging/WriteFSR.py;$APPCONFIGOPTS/Merging/MergeFSR.py 
#--  DDDB : fromPreviousStep 
#--  CONDDB : fromPreviousStep 
#--  ExtraPackages : AppConfig.v3r262 
#--  Visible : N 

from Gaudi.Configuration import * 
from GaudiConf import IOHelper
IOHelper('ROOT').inputFiles(['LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000001_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000002_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000003_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000004_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000005_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000006_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000007_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000008_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000009_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000010_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000011_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000012_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000013_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000014_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000015_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000016_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000017_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000018_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000019_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000020_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000021_1.b2kmu.trigstrip.dst',
'LFN:/lhcb/MC/2012/B2KMU.TRIGSTRIP.DST/00051296/0000/00051296_00000022_1.b2kmu.trigstrip.dst'
], clear=True)
FileCatalog().Catalogs += [ 'xmlcatalog_file:Incb_Charm2Kmu.xml' ]