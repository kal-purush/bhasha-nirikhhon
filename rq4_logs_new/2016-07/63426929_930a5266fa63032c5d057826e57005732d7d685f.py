

from Gaudi.Configuration import *
from PhysSelPython.Wrappers import Selection, SelectionSequence, DataOnDemand



from Configurables import DecayTreeTuple, FitDecayTrees, TupleToolRecoStats, TupleToolTrigger, TupleToolTISTOS, CondDB, SelDSTWriter
from DecayTreeTuple.Configuration import *

from Configurables import TrackSmeared
TrackSmeared("TrackSmearing").smearBest = True
TrackSmeared("TrackSmearing").Scale = 0.5
TrackSmearingSeq = GaudiSequencer("TrackSmearingSeq")
TrackSmearingSeq.Members = [ TrackSmeared("TrackSmearing") ]


# Standard stripping20 
name = 'bukmumu'

"""
Options for building Stripping20,
with tight track chi2 cut (<3)
"""

from Configurables import EventNodeKiller
eventNodeKiller = EventNodeKiller('Stripkiller')
eventNodeKiller.Nodes = ['/Event/AllStreams','/Event/Strip']

from Gaudi.Configuration import *
MessageSvc().Format = "% F%30W%S%7W%R%T %0W%M"

# Tighten Trk Chi2 to <3
from CommonParticles.Utils import DefaultTrackingCuts
DefaultTrackingCuts().Cuts  = { "Chi2Cut" : [ 0, 4 ],
                                "CloneDistCut" : [5000, 9e+99 ] }

#
# Build the streams and stripping object
#
from StrippingArchive.Stripping20r0p3.StrippingB2XMuMu import B2XMuMuConf as builder
from StrippingArchive.Stripping20r0p3.StrippingB2XMuMu import defaultConfig as config
from StrippingConf.Configuration import StrippingConf, StrippingStream
#from StrippingSettings.Utils import strippingConfiguration
#from StrippingArchive.Utils import buildStreams, cloneLinesFromStream
#from StrippingArchive import strippingArchive
config['MuonPID'] = -999999

stripping='stripping20'
#get the configuration dictionary from the database
#config  = strippingConfiguration(stripping)
#config['HLT_FILTER_HMuNu']=""
lb = builder('B2XMuMu',config)
print config
#get the line builders from the archive


#
# Merge into one stream and run in flag mode
#
AllStreams = StrippingStream("Dimuon")

for line in lb.lines():
    print line.name()
    if line.name() == 'StrippingB2XMuMu_InclDiMuHighQ2Line':
        AllStreams.appendLines([line])


sc = StrippingConf( Streams = [ AllStreams ],
                    MaxCandidates = 2000
                    )




tuple = DecayTreeTuple("Jpsi_Tuple")

tuple.Inputs = ["Phys/B2XMuMu_InclDiMuHighQ2Line/Particles/"]


tuple.ToolList =  [
      "TupleToolKinematic"
    , "TupleToolEventInfo"
    , "TupleToolRecoStats"
    , "TupleToolMCTruth"
    , "TupleToolMCBackgroundInfo"
   # , "TupleBuKmmFit"
]


tuple.addBranches ({         
      "muplus" :  "B0 -> ^mu+ mu-",
      "muminus" :  "B0 -> mu+ ^mu-",
      "Dimuon" : "B0 : B0 -> mu+ mu-",
})
LoKi_All=tuple.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_All")
LoKi_All.Variables = {
        'MINIPCHI2' : "MIPCHI2DV(PRIMARY)", 
        'MINIP' : "MIPDV(PRIMARY)",
        'IPCHI2_OWNPV' : "BPVIPCHI2()", 
        'IP_OWNPV' : "BPVIP()"
}

LoKi_muplus=tuple.muplus.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_muplus")
LoKi_muplus.Variables = {
       'PIDmu' : "PIDmu",
       'ghost' : "TRGHP",
       'TRACK_CHI2' : "TRCHI2DOF",
       'NNK' : "PPINFO(PROBNNK)",
       'NNpi' : "PPINFO(PROBNNpi)",
       'NNmu' : "PPINFO(PROBNNmu)"
}

LoKi_muminus=tuple.muminus.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_muminus")
LoKi_muminus.Variables = {
       'PIDmu' : "PIDmu",
       'ghost' : "TRGHP",
       'TRACK_CHI2' : "TRCHI2DOF",
       'NNK' : "PPINFO(PROBNNK)",
       'NNpi' : "PPINFO(PROBNNpi)",
       'NNmu' : "PPINFO(PROBNNmu)"
}


LoKi_Dimuon=tuple.Dimuon.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_Dimuon")
LoKi_Dimuon.Variables = {
       'DTF_CHI2' : "DTF_CHI2NDOF(True)",
       'TAU' : "BPVLTIME()",
       'DIRA_OWNPV' : "BPVDIRA",
       'FD_CHI2' : "BPVVDCHI2",
       'ENDVERTEX_CHI2' : "VFASPF(VCHI2/VDOF)",
}


list = [
      "L0DiMuonDecision"
    , "L0MuonDecision"
    , "Hlt1TrackAllL0Decision"
    , "Hlt1TrackMuonDecision"
    , "Hlt1DiMuonLowMassDecision"
    , "Hlt1DiMuonHighMassDecision"
    , "Hlt1SingleMuonHighPTDecision"
    , "Hlt2TopoMu2BodyBBDTDecision"
    , "Hlt2TopoMu3BodyBBDTDecision"
    , "Hlt2Topo2BodyBBDTDecision"
    , "Hlt2Topo3BodyBBDTDecision"
    , "Hlt2DiMuonDetachedDecision"
    , "Hlt2SingleMuonDecision"
    , "Hlt2DiMuonDetachedHeavyDecision"
]

tuple.Dimuon.ToolList += [ "TupleToolTISTOS" ]
tuple.Dimuon.addTool( TupleToolTISTOS, name = "TupleToolTISTOS" )
tuple.Dimuon.TupleToolTISTOS.Verbose = True
tuple.Dimuon.TupleToolTISTOS.TriggerList = list


tuple.Decay = "B0 -> ^mu+ ^mu-"


dstWriter = SelDSTWriter('BuKmumuDSTWriter',
                   SelectionSequences = sc.activeStreams(),
                   OutputFileSuffix = 'Stripped')

from Configurables import DaVinci
DaVinci().TupleFile = "BuKMuMu.root"

DaVinci().EvtMax = -1
DaVinci().DataType = '2011'
DaVinci().Simulation   = True
DaVinci().Lumi = True
#CondDB().UseOracle = True
DaVinci().DDDBtag = "MC11-20111102"
DaVinci().CondDBtag = "sim-20111111-vc-md100"
_myseq = GaudiSequencer("myseq")
#_myseq.Members += [ DecayTreeFitterB]
_myseq.Members += [ eventNodeKiller ]
#_myseq.Members += [ TrackSmearingSeq ]  
_myseq.Members += [ sc.sequence() ] 
#_myseq.Members += [ dstWriter.sequence() ]
_myseq.Members += [tuple]

DaVinci().UserAlgorithms = [_myseq]

DaVinci().MainOptions  = ""