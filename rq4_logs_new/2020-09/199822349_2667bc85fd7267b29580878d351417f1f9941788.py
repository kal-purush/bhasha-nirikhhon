#!/usr/bin/env python3
import argparse
import h5py
import numpy as np
import sys
import uproot
from glob import glob
from math import ceil
import numba
import numpy, numba, awkward, awkward.numba

if sys.version_info[0] < 3: sys.exit()

parser = argparse.ArgumentParser()
parser.add_argument('input', nargs='+', action='store', type=str, help='input file names')
parser.add_argument('-o', '--output', action='store', type=str, help='output file name', required=True)
parser.add_argument('-n', '--nevent', action='store', type=int, default=-1, help='number of events to preprocess')
parser.add_argument('-w', '--width', action='store', type=float, default=224, help='image width')
parser.add_argument('-h', '--height', action='store', type=float, default=224, help='image height')
parser.add_argument('--format', action='store', choices=('NHWC', 'NCHW'), default='NCHW', help='image format for output (NHWC for TF default, NCHW for pytorch default)')
parser.add_argument('-c', '--chunk', action='store', type=int, default=1024, help='chunk size')
parser.add_argument('--compress', action='store', choices=('gzip', 'lzf', 'none'), default='none', help='compression algorithm')
parser.add_argument('-d', '--debug', action='store_true', default=False, help='debugging')
parser.add_argument('--precision', action='store', type=int, choices=(8,16,32,64), default=32, help='Precision')
parser.add_argument('--dotrackcount', action='store_true', default=False, help='Choose track count for the 3rd channel, rather than the track pt')
parser.add_argument('--mc', action='store_true', default=True, help='flag to set MC sample')
args = parser.parse_args()

###################################################################################################

@numba.njit(nogil=True, fastmath=True, parallel=True)
def selectBaselineCuts(src_fjets_pt, src_fjets_eta, src_fjets_mass,
                       src_jets_pt, src_jets_eta, src_jets_btag):
    nEvent = int(len(src_fjets_pt))
    selEvents = []

    prange = numba.prange
    for ievt in prange(nEvent):
        selJets = (src_jets_pt[ievt] > 30) & (np.fabs(src_jets_eta[ievt]) < 2.4)
        if selJets.sum() < 4: continue ## require nJets >= 4
        ht = (src_jets_pt[ievt][selJets]).sum()
        if ht < 1500: continue ## require HT >= 1500

        selBJets = (src_jets_btag[ievt][selJets] > 0.5)
        if selBJets.sum() < 1: continue ## require nBJets >= 1

        selFjets = (src_fjets_pt[ievt] > 30)
        sumFjetsMass = (src_fjets_mass[ievt][selFjets]).sum()
        if sumFjetsMass < 500: continue ## require sum(FatJetMass) >= 500

        selEvents.append(ievt)

    return np.array(selEvents, dtype=np.dtype('int64'))

###################################################################################################

class FileSplitOut:
    def __init__(self, maxEvent, nEventTotal, args):
        self.maxEvent = maxEvent
        self.nEventTotal = nEventTotal

        self.chunkSize = args.chunk
        self.doTrackCount = args.dotrackcount
        self.debug = args.debug

        if args.format == 'NHWC': ## TF default
            self.shape = [args.h, args.w, 3]
            self.chAxis = -1
        else: ## pytorch default
            self.shape = [3, args.h, args.w]
            self.chAxis = 1

        precision = 'f%d' % (args.precision//8)
        self.kwargs = {'dtype':precision}
        if args.compress == 'gzip':
            self.kwargs.update({'compression':'gzip', 'compression_opts':9})
        elif args.compress == 'lzf':
            self.kwargs.update({'compression':'lzf'})

        if not args.output.endswith('.h5'): self.prefix, self.suffix = args.output+'/data', '.h5'
        else: self.prefix, self.suffix = args.output.rsplit('.', 1)

        self.nOutFile = 0
        self.nOutEvent = 0

        self.initOutput()

    def initOutput(self):
        ## Build placeholder for the output
        self.weights = np.ones(0)
        self.images = np.ones([0,*self.shape])

    def addEvents(self, src_weights, src_images_h, src_images_e, src_images_t):
        nSrcEvent = len(src_weights)
        begin = 0
        while begin < nSrcEvent:
            end = min(nSrcEvent, begin+self.maxEvent-len(self.weights))
            self.nOutEvent += (end-begin)
            print("%d/%d" % (self.nOutEvent, self.nEventTotal), end='\r')

            images_h = np.expand_dims(src_images_h[begin:end,:,:], self.chAxis)
            images_e = np.expand_dims(src_images_e[begin:end,:,:], self.chAxis)
            images_t = np.expand_dims(src_images_t[begin:end,:,:], self.chAxis)
            images = np.concatenate([images_h, images_e, images_t], axis=self.chAxis)

            self.weights = np.concatenate([self.weights, src_weights[begin:end]])
            self.images  = np.concatenate([self.images , images])

            if len(self.weights) == self.maxEvent: self.flush()
            begin = end

    def flush(self):
        self.save()
        self.initOutput()

    def save(self):
        fName = "%s_%d.h5" % (self.prefix, self.nOutFile)
        nEventToSave = len(self.weights)
        if nEventToSave == 0: return
        if self.debug: print("Writing output file %s..." % fName, end='')

        chunkSize = min(self.chunkSize, nEventToSave)
        with h5py.File(fName, 'w', libver='latest', swmr=True) as outFile:
            g = outFile.create_group('all_events')
            g.create_dataset('weights', data=self.weights, chunks=(chunkSize,), dtype='f4')
            g.create_dataset('images' , data=self.images , chunks=((chunkSize,)+self.shape), **self.kwargs)
            if self.debug: print("  done")

        self.nOutFile += 1

        if self.debug:
            with h5py.File(fName, 'r', libver='latest', swmr=True) as outFile:
                print("  created %s %dth file" % (fName, self.nOutFile), end='')
                print("  keys=", list(outFile.keys()), end='')
                print("  shape=", outFile['all_events']['images'].shape)

###################################################################################################

## Find root files with corresponding trees
print("@@@ Checking input files... (total %d files)" % (len(args.input)))
nEventTotal = 0
nEvent0s = []
srcFileNames = []
for x in args.input:
    for fName in glob(x):
        if not fName.endswith('.root'): continue
        f = uproot.open(fName)
        if treeName not in f: continue
        tree = f[treeName]
        if tree == None: continue

        if args.debug and nEventTotal == 0:
            print("-"*40)
            print("\t".join([str(key) for key in tree.keys()]))
            print("\t".join([str(key) for key in tree["Jet"].keys()]))
            print("-"*40)

        srcFileNames.append(fName)
        nEvent0 = len(tree)
        nEvent0s.append(nEvent0)
        nEventTotal += nEvent0
nEventOutFile = min(nEventTotal, args.nevent) if args.nevent >= 0 else nEventTotal

fileOuts = FileSplitOut(nEventOutFile, nEventTotal, args)

print("@@@ Start processing...")
weightName = None if args.mc else "Weight"
for nEvent0, srcFileName in zip(nEvent0s, srcFileNames):
    if args.debug: print("@@@ Open file", srcFileName)
    ## Open data files
    fin = uproot.open(srcFileName)
    tree = fin["Delphes"]

    ## Load objects
    src_weights = np.ones(nEvent0) if weightName else tree[weightName]
    src_jets_eta = tree["Jet"]["Jet.Eta"].array()
    src_jets_phi = tree["Jet"]["Jet.Phi"].array()
    src_jets_feats = [tree["Jet"][featName].array() for featName in featNames]

    ## Apply event selection in this file
    src_fjets_pt   = tree["FatJet"]["FatJet.PT"].array()
    src_fjets_eta  = tree["FatJet"]["FatJet.Eta"].array()
    src_fjets_mass = tree["FatJet"]["FatJet.Mass"].array()
    src_jets_pt   = tree["Jet"]["Jet.PT"].array()
    src_jets_btag = tree["Jet"]["Jet.BTag"].array()
    selEvent = selectBaselineCuts(src_fjets_pt, src_fjets_eta, src_fjets_mass,
                                  src_jets_pt, src_jets_eta, src_jets_btag)

    src_weights = src_weights[selEvent]

    ## Save output
    fileOuts.addEvents(src_weights)
## save remaining events
fileOuts.flush()

print("@@@ Finished processing")
print("    Number of input files   =", len(srcFileNames))
print("    Number of input events  =", nEventTotal)
print("    Number of output files  =", fileOuts.nOutFile)
print("    Number of output events =", fileOuts.nOutEvent)

###################################################################################################
