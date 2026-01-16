
"""
Authors : P.Owen, Y.Amhis 
When : July 2016
What : Option file to make Data tuples. 
First re-run the stripping, add and add a kaon. Easy enough 



"""

from Gaudi.Configuration import *
from PhysSelPython.Wrappers import Selection, SelectionSequence, DataOnDemand
from GaudiConfUtils.ConfigurableGenerators import FilterDesktop, CombineParticles




from StrippingConf.Configuration import StrippingConf, StrippingStream
from StrippingSettings.Utils import strippingConfiguration
from StrippingArchive.Utils import buildStreams
from StrippingArchive import strippingArchive





from Configurables import DecayTreeTuple, FitDecayTrees, TupleToolRecoStats, TupleToolTrigger, TupleToolTISTOS, CondDB, SelDSTWriter
from DecayTreeTuple.Configuration import *
from PhysSelPython.Wrappers import MergedSelection

# 
name = 'bukmumu'

"""
Options for building Stripping21 (was 20, but we want 21)

"""

# Build the streams and stripping object
# WARNING : the Stripping version needs to be updated 


#from StrippingArchive.Stripping20r0p3.StrippingB2XMuMu import B2XMuMuConf as builder
#from StrippingArchive.Stripping20r0p3.StrippingB2XMuMu import defaultConfig as config


#config['HLT_FILTER_HMuNu']=""





# But what we really want is to make a dimuon and a Kaon  
from StandardParticles import StdLooseKaons as kaons

LowQ2MuonsOnDemand = DataOnDemand(Location = "/Event/Dimuon/Phys/B2XMuMuIncl_InclDiMuLowQ2Line/Particles")
HighQ2MuonsOnDemand = DataOnDemand(Location = "/Event/Dimuon/Phys/B2XMuMuIncl_InclDiMuHighQ2Line/Particles")


bothstripping = MergedSelection("Selection_mergeddaughters",
       RequiredSelections = [LowQ2MuonsOnDemand,HighQ2MuonsOnDemand])

_filterDimuons = FilterDesktop(Code="ABSID==511") # Dimuons from B0--> mu mu stripping selection
_selDimuons= Selection( "_selDimuons", Algorithm = _filterDimuons, RequiredSelections = [bothstripping] )

from Configurables import SubstitutePID
subalg = SubstitutePID("_B2Jpsi_SubPID", Code="(DECTREE('B0 -> mu+ mu-'))",
                       Substitutions={'B0 -> mu+ mu-' : 'J/psi(1S)'}, MaxChi2PerDoF=-666)
subsel = Selection("subsel",Algorithm = subalg, RequiredSelections = [_selDimuons])

# Try and make B->J/psi K
_B = CombineParticles()
_B.DaughtersCuts = { "K+" : "(PT>500*MeV)&(MIPCHI2DV(PRIMARY) > 9)" }
_B.MotherCut = "(DMASS('B+')<5000*MeV) & (VFASPF(VCHI2)/VFASPF(VDOF)<5.0) & (BPVDIRA > 0.999)" #need to check these cuts
_B.DecayDescriptors = [ "[B+ -> J/psi(1S) K+]cc" ] 


_BdecaySelection = Selection( "TurboB", Algorithm = _B, RequiredSelections = [subsel,kaons] )
SeqB = SelectionSequence('SeqB', TopSelection = _BdecaySelection)

# Here we just put the output candidates in an Tuple

tupleB = DecayTreeTuple("Bplus_Tuple")
tupleB.Inputs = [SeqB.outputLocation()]
tupleB.Decay = "[B+ -> ^K+ ^(J/psi(1S) -> ^mu+ ^mu-)]CC"


tupleB.ToolList =  [
      "TupleToolKinematic"
    , "TupleToolEventInfo"
    , "TupleToolRecoStats"
   # , "TupleBuKmmFit"
] # Probably need to add many more Tools. 


tupleB.addBranches ({         
      "Kplus" :  "[B+ -> ^K+ (J/psi(1S) -> mu+ mu-)]CC",
      "Jpsi" :  "[B+ -> K+ ^(J/psi(1S) -> mu+ mu-)]CC",
      "muplus" :  "[B+ -> K+ (J/psi(1S) -> ^mu+ mu-)]CC",
      "muminus" :  "[B+ -> K+ (J/psi(1S) -> mu+ ^mu-)]CC",
      "Bplus" : "[B+ -> K+ J/psi(1S)]CC",
})



LoKi_All=tupleB.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_All")
LoKi_All.Variables = {
        'MINIPCHI2' : "MIPCHI2DV(PRIMARY)", 
        'MINIP' : "MIPDV(PRIMARY)",
        'IPCHI2_OWNPV' : "BPVIPCHI2()", 
        'IP_OWNPV' : "BPVIP()"
}




LoKi_muplus=tupleB.muplus.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_muplus")
LoKi_muplus.Variables = {
       'PIDmu' : "PIDmu",
       'ghost' : "TRGHP",
       'TRACK_CHI2' : "TRCHI2DOF",
       'NNK' : "PPINFO(PROBNNK)",
       'NNpi' : "PPINFO(PROBNNpi)",
       'NNmu' : "PPINFO(PROBNNmu)"
}

LoKi_Kplus=tupleB.Kplus.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_Kplus")
LoKi_Kplus.Variables = {
       'PIDmu' : "PIDmu",
       'PIDK' : "PIDK",
       'ghost' : "TRGHP",
       'TRACK_CHI2' : "TRCHI2DOF",
       'NNK' : "PPINFO(PROBNNK)",
       'NNpi' : "PPINFO(PROBNNpi)",
       'NNmu' : "PPINFO(PROBNNmu)"
}
LoKi_muminus=tupleB.muminus.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_muminus")
LoKi_muminus.Variables = {
       'PIDmu' : "PIDmu",
       'ghost' : "TRGHP",
       'TRACK_CHI2' : "TRCHI2DOF",
       'NNK' : "PPINFO(PROBNNK)",
       'NNpi' : "PPINFO(PROBNNpi)",
       'NNmu' : "PPINFO(PROBNNmu)"
}


LoKi_B=tupleB.Bplus.addTupleTool("LoKi::Hybrid::TupleTool/LoKi_B")
LoKi_B.Variables = {
       'DTF_CHI2' : "DTF_CHI2NDOF(True)",
       'TAU' : "BPVLTIME()",
       'DIRA_OWNPV' : "BPVDIRA",
       'FD_CHI2' : "BPVVDCHI2",
       'ENDVERTEX_CHI2' : "VFASPF(VCHI2/VDOF)",
       'PVX' : "BPV(VX)",
       'PVY' : "BPV(VY)",
       'PVZ' : "BPV(VZ)",
       'VX' : "VFASPF(VX)",
       'VY' : "VFASPF(VY)",
       'VZ' : "VFASPF(VZ)",
       'X_travelled' : "VFASPF(VX)-BPV(VX)",
       'Y_travelled' : "VFASPF(VY)-BPV(VY)",
       'Z_travelled' : "VFASPF(VZ)-BPV(VZ)",
       'P_Parallel' : "BPVDIRA*P",
       'P_Perp' : "sin(acos(BPVDIRA))*P",
       'Corrected_Mass' : "BPVCORRM"
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
] #Is the trigger list uptodate? 


tupleB.Bplus.ToolList += [ "TupleToolTISTOS" ]
tupleB.Bplus.addTool( TupleToolTISTOS, name = "TupleToolTISTOS" )
tupleB.Bplus.TupleToolTISTOS.Verbose = True
tupleB.Bplus.TupleToolTISTOS.TriggerList = list


tupleB.Jpsi.ToolList += [ "TupleToolTISTOS" ]
tupleB.Jpsi.addTool( TupleToolTISTOS, name = "TupleToolTISTOS" )
tupleB.Jpsi.TupleToolTISTOS.Verbose = True
tupleB.Jpsi.TupleToolTISTOS.TriggerList = list





from Configurables import DaVinci
DaVinci().TupleFile = "BuKMuMu.root"
DaVinci().EvtMax = -1
DaVinci().DataType = '2012'
DaVinci().Simulation   = False
DaVinci().Lumi = not DaVinci().Simulation

_myseq = GaudiSequencer("myseq")
_myseq.Members += [SeqB.sequence() ] # make B candidates (muon channel)
_myseq.Members +=[ tupleB] # put stuff in a Tuple
DaVinci().UserAlgorithms = [_myseq] # run the whole thing
DaVinci().MainOptions  = ""



"""
#we should put this back later if we want to smear stuff
from Configurables import TrackSmeared
TrackSmeared("TrackSmearing").smearBest = True
TrackSmeared("TrackSmearing").Scale = 0.5
TrackSmearingSeq = GaudiSequencer("TrackSmearingSeq")
TrackSmearingSeq.Members = [ TrackSmeared("TrackSmearing") ]
"""
#try to do it like in the starterkit 

"""


# Build a new stream called 'CustomStream' that only
# contains the desired line
strip = 'stripping21'
streams = buildStreams(stripping=strippingConfiguration(strip),
                       archive=strippingArchive(strip))

custom_stream = StrippingStream('CustomStream')
custom_line = 'B2XMuMu_InclDiMuHighQ2Line'

for stream in streams:
    for line in stream.lines:
        if line.name() == custom_line:
            custom_stream.appendLines([line])

line = 'B2XGamma2pi_wCNV_Line'
# Create the actual Stripping configurable
filterBadEvents = ProcStatusCheck()
sc = StrippingConf(Streams=[custom_stream],
                   MaxCandidates=2000,
                   AcceptBadEvents=False,
                   BadEventSelection=filterBadEvents)
# The output is placed directly into Phys, so we only need to
# define the stripping line here
line =  'B2XGamma2pi_wCNV_Line'
# Stream and stripping line we want to use
tesLoc = '/Event/Phys/{0}/Particles'.format(line)
# get the selection(s) created by the stripping
strippingSels = [DataOnDemand(Location=tesLoc)]


"""