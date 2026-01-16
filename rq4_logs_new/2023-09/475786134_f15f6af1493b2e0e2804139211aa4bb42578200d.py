#!/usr/bin/env python3

import argparse
parser = argparse.ArgumentParser(description='Vcb_Post_Analysis Command')
parser.add_argument('-e', dest='era', default="2017")
parser.add_argument('-ch', dest='channel', default="MM")
args = parser.parse_args()

if args.era=="2016a": args.era="2016preVFP"
if args.era=="2016b": args.era="2016postVFP"

import ROOT
import os
path=os.environ['Vcb_Post_Analysis_WD']
ROOT.gSystem.Load(f"{path}/Build/libVcb_Post_Analysis.so")

try:
    cr_dl = ROOT.CR_DL(args.era, args.channel)
except:
    print("Exception")
else:
    cr_dl.Run()