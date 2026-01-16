import FWCore.ParameterSet.Config as cms
from subprocess import *
import FWCore.Utilities.FileUtils as FileUtils
mylist=FileUtils.loadListFromFile('/afs/cern.ch/user/m/mshi/CMSSW_7_6_3/src/GGHAA2Mu2TauAnalysis/Yohay_h125.txt')
process = cms.Process("SKIM")

#PDG IDs
A_PDGID = 36
Z_PDGID = 23
W_PDGID = 24
TAU_PDGID = 15
MU_PDGID = 13
NUMU_PDGID = 14
D_PDGID = 1
U_PDGID = 2
S_PDGID = 3
C_PDGID = 4
B_PDGID = 5
T_PDGID = 6
G_PDGID = 21
ANY_PDGID = 0

#tau decay types
TAU_HAD = 0
TAU_MU = 1
TAU_E = 2
TAU_ALL = 3

#tau hadronic decay types
TAU_ALL_HAD = -1
TAU_1PRONG_0NEUTRAL = 0
TAU_1PRONG_1NEUTRAL = 1
TAU_1PRONG_2NEUTRAL = 2
TAU_1PRONG_3NEUTRAL = 3
TAU_1PRONG_NNEUTRAL = 4
TAU_2PRONG_0NEUTRAL = 5
TAU_2PRONG_1NEUTRAL = 6
TAU_2PRONG_2NEUTRAL = 7
TAU_2PRONG_3NEUTRAL = 8
TAU_2PRONG_NNEUTRAL = 9
TAU_3PRONG_0NEUTRAL = 10
TAU_3PRONG_1NEUTRAL = 11
TAU_3PRONG_2NEUTRAL = 12
TAU_3PRONG_3NEUTRAL = 13
TAU_3PRONG_NNEUTRAL = 14
TAU_RARE = 15

#no consideration of pT rank
ANY_PT_RANK = -1

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = cms.untracked.int32(1000)

process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True),
                SkipEvent = cms.untracked.vstring('ProductNotFound'))

process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32(10000) )
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring(*mylist))

process.source.inputCommands = cms.untracked.vstring("keep *")

#for L1GtStableParametersRcd
process.load('Configuration/StandardSequences/FrontierConditions_GlobalTag_cff')

#for HLT selection
process.load('HLTrigger/HLTfilters/hltHighLevel_cfi')
import HLTrigger.HLTfilters.hltHighLevel_cfi as hlt
#for mu-less jets
process.load('Configuration.StandardSequences.MagneticField_cff') #I changed it from: process.load("Configuration.StandardSequences.MagneticField_38T_cff")
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_condDBv2_cff') # Kyle Added this
process.load('TrackingTools.TransientTrack.TransientTrackBuilder_cfi') # Kyle Added this
process.GlobalTag.globaltag = cms.string('76X_dataRun2_v15') # Kyle added this
process.load("Configuration.Geometry.GeometryRecoDB_cff")
process.load("RecoTauTag.Configuration.RecoPFTauTag_cff")
process.load("RecoTauTag.RecoTau.RecoTauPiZeroProducer_cfi")
process.load('Tools/CleanJets/cleanjets_cfi')

#define a parameter set to be passed to all modules that utilize GenTauDecayID for signal taus
AMuMuPSet = cms.PSet(momPDGID = cms.vint32(A_PDGID),
                                   chargedHadronPTMin = cms.double(0.0),
                                   neutralHadronPTMin = cms.double(0.0),
                                   chargedLeptonPTMin = cms.double(0.0),
                                   totalPTMin = cms.double(0.0))

# load the PAT config
process.load("PhysicsTools.PatAlgos.patSequences_cff")

# Configure PAT to use PF2PAT instead of AOD sources
# this function will modify the PAT sequences. 
from PhysicsTools.PatAlgos.tools.pfTools import *
from PhysicsTools.PatAlgos.tools.metTools import *


# to use tau-cleaned jet collection uncomment the following: 
#getattr(process,"pfNoTau"+postfix).enable = True

# to switch default tau to HPS tau uncomment the following: 
#adaptPFTaus(process,"hpsPFTau",postfix=postfix)


#output commands
skimEventContent = cms.PSet(
    outputCommands = cms.untracked.vstring(
    "keep *",
    "drop *_*ak7*_*_*",
    "drop *_*ak5*_*_*",
    "drop *_*ak8*_*_*",
    "drop *_*GenJets_*_*",
    "drop *_ak4CaloJets*_*_*",
    "drop *_ak4TrackJets*_*_*",
    "drop *_*jetCentral*_*_*",
    "drop *_fixedGridRho*_*_*",
    "drop *_hfEMClusters_*_*",
    "drop *_eid*_*_*",
    "drop *_muonMETValueMapProducer_muCorrData_*",
    "drop *_muons_muonShowerInformation_*",
    "drop *_muons_combined_*",
    "drop *_muons_csc_*",
    "drop *_muons_dt_*",
    "drop l1extraL1HFRings_*_*_*",
    "drop *_muonsFromCosmics*_*_*",
    "drop recoCaloClusters_*_*_*",
    "drop recoPreshowerCluster*_*_*_*",
    "drop *_hfRecoEcalCandidate_*_*",
    "drop *_generalV0Candidates_*_*",
    "drop *_selectDigi_*_*",
    "drop *_*BJetTags*_*_RECO",
    "drop *_castorreco_*_*",
    "drop *_reduced*RecHits*_*_*",
    "drop *_PhotonIDProd_*_*",
    "drop *_*_*photons*_*",
    "drop *_dedx*_*_*",
    "drop *_*_cosmicsVeto_*",
    "drop *_muonMETValueMapProducer_*_*",
    "drop *_BeamHaloSummary_*_*",
    "drop *_GlobalHaloData_*_*",
    "drop *_*_uncleanOnly*_*",
    "drop recoCaloMET_*_*_*",
    "drop recoConversion_*_*_*",
    "drop *_CastorTowerReco_*_*",
    "drop *_uncleanedOnlyGsfElectron*_*_*",
    "drop recoJPTJet_*_*_*",
    "drop recoPFMET_*_*_*",
##     "drop *_photons_*_*",
##     "drop *_photonCore_*_*",

    "drop *_hpsPFTauDiscrimination*_*_RECO",
    "drop *_hpsPFTauProducer_*_RECO",
    "drop *_recoTauAK4PFJets08Region_*_SKIM",
    "drop *_ak4PFJetTracksAssociatorAtVertex_*_SKIM",
    "drop *_ak4PFJetsLegacyHPSPiZeros_*_SKIM",
    "drop *_combinatoricRecoTausDiscriminationByLeadingPionPtCut_*_SKIM",
    "drop *_combinatoricRecoTausHPSSelector_*_SKIM",
    "drop *_hpsSelectionDiscriminator_*_SKIM",
    "drop *_combinatoricRecoTaus_*_SKIM",
    "drop *_hpsPFTauProducerSansRefs_*_SKIM",
    "drop *_pfRecoTauTagInfoProducer_*_SKIM",
    "drop *_recoTauPileUpVertices_*_SKIM",
    "drop *_correctedHybridSuperClusters_*_*",
    "drop *_correctedMulti5x5SuperClustersWithPreshower_*_*",
    "drop *_*phPFIsoValue*04PFIdPFIso_*_*",
    "drop *_*TagInfos*_*_*",
    "drop *_ghostTrackBJetTags_*_SKIM",
    "drop *_jet*ProbabilityBJetTags_*_SKIM",
    "drop *_simpleSecondaryVertexHigh*BJetTags_*_SKIM",
    "drop *_trackCountingHigh*BJetTags_*_SKIM",
    "drop CorrMETData_*_*_SKIM",
    "drop *_*NoNu_*_*"
    #added 2-Jul-13 after estimating data skim size
##     "drop *_clusterSummaryProducer_*_*",
##     "drop *_hcalnoise_*_*",
##     "drop *_castorDigis_*_*",
##     "drop *_hcalDigis_*_*",
##     "drop double_ak4PFJets_rho*_*",
##     "drop double_ak4PFJets_sigma*_*",
##     "drop *_tevMuons_*_*",
##     "drop recoIsoDepositedmValueMap_*_*_*",
##     "drop *_logErrorHarvester_*_*",
##     "drop *_l1extraParticles_*_*",
##     "drop *_particleFlowTmp_*_*",
##     "drop *_particleFlowCluster*_*_*",
##     "drop *_particleFlowRecHit*_*_*",
##     "drop recoPFCandidates_CleanJets_*_SKIM"
    )
  ) 

# b-tagging general configuration
process.load("RecoBTag.Configuration.RecoBTag_cff")
process.load("RecoJets.JetAssociationProducers.ak4JTA_cff")
# configure the softMuonByIP3d ESProducer and EDProducer
from RecoBTag.SoftLepton.softLepton_cff import *
from RecoBTag.ImpactParameter.impactParameter_cff import *
from RecoBTag.SecondaryVertex.secondaryVertex_cff import *
process.impactParameterTagInfos=process.impactParameterTagInfos.clone()
process.impactParameterTagInfos.jetTracks = cms.InputTag("ak4JetTracksAssociatorAtVertex")
process.ak4JetTracksAssociatorAtVertex.jets = cms.InputTag('CleanJets','ak4PFJetsNoMu','SKIM')
process.ak4JetTracksAssociatorAtVertex.tracks = cms.InputTag("generalTracks")

	
process.btagging = cms.Sequence(
    process.ak4JetTracksAssociatorAtVertex*
    # impact parameters and IP-only algorithms
    process.impactParameterTagInfos*
    (process.trackCountingHighEffBJetTags +
     process.trackCountingHighPurBJetTags +
     process.jetProbabilityBJetTags +
     process.jetBProbabilityBJetTags +
     # SV tag infos depending on IP tag infos, and SV (+IP) based algos
     process.secondaryVertexTagInfos*
     (process.simpleSecondaryVertexHighEffBJetTags +
      process.simpleSecondaryVertexHighPurBJetTags +
      process.combinedSecondaryVertexBJetTags) +
     process.ghostTrackVertexTagInfos*
     process.ghostTrackBJetTags)##  +
##     process.softPFMuonsTagInfos*
##     process.softPFMuonBJetTags *
##     process.softPFElectronsTagInfos*
##     process.softPFElectronBJetTags
)
#only proceed if event is a true W-->munu event

#require event to fire IsoMu24_eta2p1
process.MuonIWant = cms.EDFilter('MuonRefSelector',
                                 src = cms.InputTag('muons'),
                                 cut = cms.string('pt > 0.0'),
                                 filter = cms.bool(True)

)
process.HighestPtAndSecondHighestPtMuonOppositeSign=cms.EDFilter('HighestSecondHighestPtSelector',
                                                     muonTag=cms.InputTag('MuonIWant')
)
process.LooseMu1Mu2=cms.EDFilter('LooseMuon',
                               muonTag=cms.InputTag('HighestPtAndSecondHighestPtMuonOppositeSign'),
                               minNumObjsToPassFilter=cms.uint32(2)
)
process.PtEtaCut = cms.EDFilter('PTETACUT',
                                 muonTag=cms.InputTag('LooseMu1Mu2'),
                                 Eta=cms.double(2.1),
                                 Pt=cms.double(45.0),
                                 minNumObjsToPassFilter=cms.uint32(1)
)
process.filter_1 = hlt.hltHighLevel.clone(
    HLTPaths = [ 'HLT_Mu45_eta2p1_v*'],
    throw = True
    )


process.Mu45Selector = cms.EDFilter(
    'MuonTriggerObjectFilter',
    recoObjTag = cms.InputTag('PtEtaCut'),
    triggerEventTag = cms.untracked.InputTag("hltTriggerSummaryAOD", "", "HLT"),
    triggerResultsTag = cms.untracked.InputTag("TriggerResults", "", "HLT"),
    MatchCut = cms.untracked.double(0.05),
    hltTags = cms.VInputTag(cms.InputTag("HLT_Mu45_eta2p1_v2", "", "HLT")
                            ),
    theRightHLTTag = cms.InputTag("HLT_Mu45_eta2p1_v2"),
    theRightHLTSubFilter1 = cms.InputTag("hltL3fL1sMu16orMu25L1f0L2f10QL3Filtered45e2p1Q"),
   # theRightHLTSubFilter1 = cms.InputTag("hltL2fL1sMu16orMu25L1f0L2Filtered10Q"),
   # theRightHLTSubFilter1=cms.InputTag("hltL1sL1SingleMu16ORSingleMu25"),
   # theRightHLTSubFilter1=cms.InputTag("hltL1fL1sMu16orMu25L1Filtered0"),
    HLTSubFilters = cms.untracked.VInputTag(""),
    minNumObjsToPassFilter1= cms.uint32(1),
    outFileName=cms.string("DrellY_Mu45Selector.root")
)
process.afterVetoMu1Mu2 = cms.EDFilter('VetoMuon',
                              muonTag=cms.InputTag('MuonIWant'),
                              vetoMuonTag=cms.InputTag('LooseMu1Mu2'),
                              minNumObjsToPassFilter=cms.uint32(1)
)
#process.AMuTriggerAnalyzer=cms.EDAnalyzer(
#   'AMuTriggerAnalyzer',
#   GenMatchedRecoMuonTag= cms.InputTag('CustomMu1Selector'),
#   MuPassTrigger=cms.InputTag('Mu45Selector'),
#   GenMatchedRecoMuonTag2 = cms.InputTag('HighestPTMuon2'),
#   Bins=cms.vdouble(0.0,0.02,0.04,0.06,0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 1,2, 3,4,5)
#)
process.tauMuonSelector=cms.EDFilter('LooseMuon',
                               muonTag=cms.InputTag('afterVetoMu1Mu2'),
                               minNumObjsToPassFilter=cms.uint32(1)
)
process.tauMuonHighestPtSelector=cms.EDFilter('HighestPtSelector',
                                               muonTag=cms.InputTag('tauMuonSelector')
)# to be studied remove or not
process.tauMuonPtSelector=cms.EDFilter('PTETACUT',
                                 muonTag=cms.InputTag('tauMuonHighestPtSelector'),
                                 Eta=cms.double(2.3),
                                 Pt=cms.double(5.0),
                                 minNumObjsToPassFilter=cms.uint32(1)
)
#search for a muon with pT > 25 GeV as in WHbb CMS AN-2012/349 and proceed if one can be found
#this will produce a ref to the original muon collection

#produce photon isolations

#search for a tight PF isolated tight muon in |eta| < 2.1 with pT > 25 GeV
#(see https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideMuonId#Muon_Isolation_AN1 for
#isolation definition; CMS AN-2012/349 uses loose isolation working point for WHbb muon selection)
#this will produce a ref to the original muon collection

#search for a muon with pT > 5 GeV as in HZZ4l analysis and proceed if one can be found
#this will produce a ref to the original muon collection

#search for soft muons
#(see https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideMuonId#Soft_Muon) not overlapping with
#the W muon in |eta| < 2.4
#this will produce a ref to the original muon collection

#clean the jets of soft muons, then rebuild the taus

process.CleanJets.muonSrc=cms.InputTag('tauMuonPtSelector') # 
process.CleanJets.PFCandSrc = cms.InputTag('particleFlow')
process.CleanJets.cutOnGenMatches = cms.bool(False)
process.CleanJets.outFileName = cms.string('H750a09_CleanJets.root')
process.recoTauAK4PFJets08Region.src = cms.InputTag("CleanJets", "ak4PFJetsNoMu", "SKIM")
process.ak4PFJetTracksAssociatorAtVertex.jets = cms.InputTag('CleanJets', 'ak4PFJetsNoMu', 'SKIM')
process.recoTauAK4PFJets08Region.src = cms.InputTag('CleanJets', 'ak4PFJetsNoMu', 'SKIM')
process.ak4PFJetsLegacyHPSPiZeros.jetSrc = cms.InputTag('CleanJets', 'ak4PFJetsNoMu', 'SKIM')
process.ak4PFJetsRecoTauChargedHadrons.jetSrc = cms.InputTag('CleanJets', 'ak4PFJetsNoMu', 'SKIM')
process.combinatoricRecoTaus.jetSrc = cms.InputTag('CleanJets', 'ak4PFJetsNoMu', 'SKIM')

process.recoTauCommonSequence = cms.Sequence(   process.tauMuonSelector*
                                                process.tauMuonHighestPtSelector*
                                                process.tauMuonPtSelector*
						process.CleanJets*
						process.ak4PFJetTracksAssociatorAtVertex*
						process.recoTauAK4PFJets08Region*
						process.recoTauPileUpVertices*
						process.pfRecoTauTagInfoProducer
)

process.PFTau = cms.Sequence(process.recoTauCommonSequence*process.recoTauClassicHPSSequence) # Kyle Changed  This

#find taus in |eta| < 2.4 matched to muon-tagged cleaned jets
#this will produce a ref to the cleaned tau collection
process.muHadTauSelector = cms.EDFilter(
    'CustomTauSepFromMuonSelector',
    baseTauTag = cms.InputTag('hpsPFTauProducer', '', 'SKIM'),
    tauHadIsoTag = cms.InputTag('hpsPFTauDiscriminationByCombinedIsolationDeltaBetaCorrRaw3Hits', '',
                                'SKIM'),
    tauDiscriminatorTags = cms.VInputTag(
    cms.InputTag('hpsPFTauDiscriminationByDecayModeFindingNewDMs', '', 'SKIM'),
    cms.InputTag('hpsPFTauDiscriminationByMediumCombinedIsolationDBSumPtCorr3Hits','','SKIM')
    ),
    jetTag = cms.InputTag('CleanJets', 'ak4PFJetsNoMu', 'SKIM'),
    muonRemovalDecisionTag = cms.InputTag('CleanJets','valMap','SKIM'),  
    overlapCandTag = cms.InputTag('Mu45Selector'),
    overlapCandTag1= cms.InputTag('LooseMu1Mu2'),#this module has a selection efficiency 5%, but comment this line out, rate goes up to 80%.
    passDiscriminator = cms.bool(True),
    pTMin = cms.double(20.0),
    etaMax = cms.double(2.3),
    isoMax = cms.double(-1.0),
    dR = cms.double(0.5),
    minNumObjsToPassFilter = cms.uint32(1),
    outFileName=cms.string('H750a09_muHadTauSelector.root')
    )
process.RECOAnalyze=cms.EDAnalyzer(
'MuMuTauTauRecoAnalyzer',
  tauTag=cms.InputTag('muHadTauSelector'),
  muonTag1=cms.InputTag('Mu45Selector'),
  muonTag2=cms.InputTag('LooseMu1Mu2'),
  genParticleTag=cms.InputTag('genParticles'),
 jetMuonMapTag=cms.InputTag('CleanJets','muonValMap','SKIM'),
  muHadMassBins=cms.vdouble(0.0, 2.0, 4.0, 6.0, 8.0, 10.0,12.0,14.0),
  outFileName=cms.string('H750a09_RECOAnalyzer.root')
)
#output
process.noSelectedOutput = cms.OutputModule(
    "PoolOutputModule",
    SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring('p')),
    outputCommands = skimEventContent.outputCommands,
    fileName = cms.untracked.string('H750a09_output.root')
    )

#sequences
process.MuMuSequenceSelector=cms.Sequence(
	process.MuonIWant*
        process.HighestPtAndSecondHighestPtMuonOppositeSign*
        process.LooseMu1Mu2*
        process.PtEtaCut*
        #process.filter_1 
        process.Mu45Selector*
	process.afterVetoMu1Mu2*
        process.tauMuonSelector*
        process.tauMuonHighestPtSelector*
        process.tauMuonPtSelector
#process.AMuTriggerAnalyzer
)

process.noSelectionSequence = cms.Sequence(process.MuMuSequenceSelector*
                                           process.PFTau*
                                           process.muHadTauSelector*
                                           process.btagging
                                          # process.RECOAnalyze
)

## #selection path
## process.p = cms.Path(process.selectionSequence)
## process.e = cms.EndPath(process.selectedOutput)

#anti-selection path
## process.p = cms.Path(process.antiSelectionSequence)
## process.e = cms.EndPath(process.antiSelectedOutput)
process.TFileService = cms.Service("TFileService",
    fileName =  cms.string('H750a09_Tfile.root')
)
#no selection path
process.p = cms.Path(process.noSelectionSequence)
process.e = cms.EndPath(process.noSelectedOutput)