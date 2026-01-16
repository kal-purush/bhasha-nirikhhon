#! /bin/env python

import sys, os, json
import copy
import datetime
import subprocess
import numpy as np
import glob
import ROOT

import argparse

# Add all the TH2 Mjj_vs_Mlljj histos together
def add2D(path, prefix1, prefix2, data):

    _files = set()
    histos = []

    cat = []
    cat.append("MuMu")
    cat.append("ElEl")


    for c in cat:
        if data: 
            Mjj_vs_Mlljj = ROOT.TH2F("Mjj_vs_Mlljj_" + prefix1 + "_" + c + "_data","Mjj_vs_Mlljj_" + prefix1 + "_" + c + "_data", 60, 0, 1500, 60, 0, 1500)
        else:
            Mjj_vs_Mlljj = ROOT.TH2F("Mjj_vs_Mlljj_" + prefix1 + "_" + c + "_MC","Mjj_vs_Mlljj_" + prefix1 + "_" + c + "_MC", 60, 0, 1500, 60, 0, 1500)
        Mjj_vs_Mlljj.Reset()
        for i, filename in enumerate(glob.glob(os.path.join(path, '*.root'))):
            split_filename = filename.split('/')
            if not str(split_filename[-1]).startswith(prefix1) and not str(split_filename[-1]).startswith(prefix2):
                continue
            f = ROOT.TFile.Open(filename)
            print "file: ", str(split_filename[-1])
            _files.add(f)
            histo2D = f.Get("Mjj_vs_Mlljj_" + c + "_hZA_lljj_deepCSV_btagM_mll_and_met_cut")
            #histo2D.SetDirectory(0)
            print "data: ", bool(data)
            print "cat: ", c
            print "histo2D.GetEntries(): ", histo2D.GetEntries()
            Mjj_vs_Mlljj.Add(histo2D, 1)
            Mjj_vs_Mlljj.SetDirectory(0)
            #print "Mjj_vs_Mlljj.GetName(): ", Mjj_vs_Mlljj.GetName()
            #print "Mjj_vs_Mlljj.GetEntries(): ", Mjj_vs_Mlljj.GetEntries()
        Mjj_vs_Mlljj.SetName(Mjj_vs_Mlljj.GetName() + "_" + prefix1 + "_" + c)
        histos.append(Mjj_vs_Mlljj)

    return histos


def main():

    global lumi
    lumi = 35922. 

    path = '/nfs/scratch/fynu/asaggio/CMSSW_8_0_30/src/cp3_llbb/ZATools/factories_ZA/inverted_met_cut/slurm/output/'
    Mjj_vs_Mlljj_ttbarMC = add2D(path, "TT_Other_TuneCUETP8M2T4", "TTTo2L2Nu_13TeV-powheg", data=False)
    Mjj_vs_Mlljj_ttbarMC[0].Scale(lumi)
    Mjj_vs_Mlljj_ttbarMC[1].Scale(lumi)
    
    Mjj_vs_Mlljj_ttbarData = add2D(path, "ttbar_from_data", "ttbar_from_data", data=True)
    # Scale the ttbar from data by the lumi (since the histo is already scale by 1/lumi to accommodate plotIt)
    Mjj_vs_Mlljj_ttbarData[0].Scale(lumi)
    Mjj_vs_Mlljj_ttbarData[1].Scale(lumi)
    
    Mjj_vs_Mlljj_DY = add2D(path, "DY", "DY", data=False)
    Mjj_vs_Mlljj_DY[0].Scale(lumi)
    Mjj_vs_Mlljj_DY[1].Scale(lumi)

    print "ttbar MC, cat. MuMu: ", Mjj_vs_Mlljj_ttbarMC[0].Integral()
    print "ttbar MC, cat. ElEl: ", Mjj_vs_Mlljj_ttbarMC[1].Integral()
    print "ttbar data, cat. MuMu: ", Mjj_vs_Mlljj_ttbarData[0].Integral()
    print "ttbar data, cat. ElEl: ", Mjj_vs_Mlljj_ttbarData[1].Integral()
    print "DY, cat. MuMu: ", Mjj_vs_Mlljj_DY[0].Integral()
    print "DY, cat. ElEl: ", Mjj_vs_Mlljj_DY[1].Integral()

    Mjj_vs_Mlljj_ttbarMC[0].GetZaxis().SetRangeUser(0, 450)
    Mjj_vs_Mlljj_ttbarMC[1].GetZaxis().SetRangeUser(0, 90)
    Mjj_vs_Mlljj_ttbarData[0].GetZaxis().SetRangeUser(0, 450)
    Mjj_vs_Mlljj_ttbarData[1].GetZaxis().SetRangeUser(0, 90)
    Mjj_vs_Mlljj_DY[0].GetZaxis().SetRangeUser(0, 1200)
    Mjj_vs_Mlljj_DY[1].GetZaxis().SetRangeUser(0, 300)
    
    output_dir = os.path.join(os.getcwd(), "checkStats")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    r_file = ROOT.TFile.Open("checkStats/compareStatsTTbarMC_vs_Data.root", "recreate")
    Mjj_vs_Mlljj_ttbarMC[0].Write()
    r_file.cd()
    Mjj_vs_Mlljj_ttbarMC[1].Write()
    r_file.cd()
    Mjj_vs_Mlljj_ttbarData[0].Write()
    r_file.cd()
    Mjj_vs_Mlljj_ttbarData[1].Write()
    r_file.cd()
    Mjj_vs_Mlljj_DY[0].Write()
    r_file.cd()
    Mjj_vs_Mlljj_DY[1].Write()
    r_file.cd()
    r_file.Close()

#main
if __name__ == "__main__":
    ROOT.gROOT.SetBatch(True)
    main()
