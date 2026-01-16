import sys
import ROOT

ROOT.gSystem.Load('./bin/libBTAnalysis.so')

#input filename
inFile=sys.argv[1]
#number of modules
nmodules=int(sys.argv[2])
#outfilename
outFile=sys.argv[1].replace(".root", "_histos.root")

analyzer=ROOT.BTAnalyzer(inFile, nmodules, outFile)
print "Executing event loop!"
analyzer.Loop()
print "Saving Histos!"
analyzer.SaveHistos()