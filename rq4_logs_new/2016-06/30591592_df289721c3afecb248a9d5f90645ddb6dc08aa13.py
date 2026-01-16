import FWCore.ParameterSet.Config as cms

process = cms.Process("LHC")

process.source = cms.Source("ErrorStreamSource",
                            fileNames = cms.untracked.vstring('file:/cmsnfshltdata/hltdata/TSG/ErrorStream/run273554/run273554_ls0369_index000165.raw'),
                            firstRun  = cms.untracked.uint32(273554)
                            )

from EventFilter.RawDataCollector.rawDataCollectorByLabel_cfi import rawDataCollector
process.rawDataCollector = rawDataCollector.clone(
        verbose = cms.untracked.int32(0),
            RawCollectionList = cms.VInputTag( cms.InputTag('source') )
        )

process.output = cms.OutputModule( "PoolOutputModule",
                                   #fileName = cms.untracked.string( "Run252488_ls1482_streamError_bu-c2e18-39-01.root" ),
                                   fileName = cms.untracked.string( "Run273554_ls0369_RAW.root" ),
                                   outputCommands = cms.untracked.vstring(
                                                                          'drop *',
                                                                          'keep *_rawDataCollector_*_*'
                                   )
)

process.raw = cms.Path( process.rawDataCollector )
process.end = cms.EndPath( process.output ) 