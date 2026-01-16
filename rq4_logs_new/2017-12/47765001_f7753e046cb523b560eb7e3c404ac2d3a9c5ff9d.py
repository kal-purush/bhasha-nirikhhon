#! /bin/env python

import sys, os, json
import copy
import datetime
import subprocess
import numpy as np
import glob
import ROOT
from ROOT import TCanvas, TPad, TLine

import argparse




def cloneTH1(hist):
    if (hist == None):
        return None
    cloneHist = ROOT.TH1F(str(hist.GetName()) + "_clone", hist.GetTitle(), hist.GetNbinsX(), hist.GetXaxis().GetXmin(), hist.GetXaxis().GetXmax())
    return cloneHist


def cloneTH2(hist):
    if (hist == None):
        return None
    cloneHist = ROOT.TH2F(str(hist.GetName()) + "_clone", hist.GetTitle(), hist.GetNbinsX(), hist.GetXaxis().GetXmin(), hist.GetXaxis().GetXmax(), hist.GetNbinsY(), hist.GetYaxis().GetXmin(), hist.GetYaxis().GetXmax()) 
    return cloneHist


def getHistos(path, prefix):

    histos_fromSameFile = []
    histos = []
    new_histos = []
    n_histos = 0

    # Get the number of histograms
    for i, filename in enumerate(glob.glob(os.path.join(path, '*.root'))):
        split_filename = filename.split('/')
        if not str(split_filename[-1]).startswith(prefix):
            continue
        f = ROOT.TFile(filename)
        del histos_fromSameFile[:]
        for j, key in enumerate(f.GetListOfKeys()):
            cl = ROOT.gROOT.GetClass(key.GetClassName())
            if not cl.InheritsFrom("TH1"):
                continue
            n_histos = j
    print "n_histos is: ", n_histos+1

    for i, filename in enumerate(glob.glob(os.path.join(path, '*.root'))):
        split_filename = filename.split('/')
        if not str(split_filename[-1]).startswith(prefix):
            continue
        f = ROOT.TFile(filename)
        del histos_fromSameFile[:]
        for j, key in enumerate(f.GetListOfKeys()):
            cl = ROOT.gROOT.GetClass(key.GetClassName())
            if not cl.InheritsFrom("TH1"):
                continue
            histos_fromSameFile.append(key.ReadObj())
            if j  == n_histos:
                histos.append(histos_fromSameFile)

    print "Number of files processed for %s: " %prefix, len(histos)
    print "Number of histograms processed for %s: " %prefix, len(histos[0])

    for j in range(0, len(histos[0])):
        if "_vs_" in str(histos[0][j].GetName()):
            new_histo = cloneTH2(histos[0][j])
        else:
            new_histo = cloneTH1(histos[0][j])
        for i in range(0, len(histos)):
            new_histo.Add(histos[i][j], 1)
            new_histo.SetDirectory(0)
        new_histos.append(new_histo)

    # Scale by the luminosity if MC histograms
    if prefix != 'MuonEG':
        for h in new_histos:
            h.Scale(lumi)
    
    return new_histos







def main():

 
    global lumi
    lumi = 35922. 
    #path = '/nfs/scratch/fynu/asaggio/CMSSW_8_0_30/src/cp3_llbb/ZATools/factories_ZA/ttbarSplitting/slurm/output/'
    path = '/nfs/scratch/fynu/asaggio/CMSSW_8_0_30/src/cp3_llbb/ZATools/factories_ZA/inverted_met_cut/slurm/output/'

    histos_data = getHistos(path, "MuonEG")
    histos_SingleTop = getHistos(path, "ST")
    est_ttbar = []

    if len(histos_data) != len(histos_SingleTop):
        print "Something went wrong: different number of histograms in data and MC!"
 
    for i in range(0, len(histos_data)):
        if "MuEl" in str(histos_data[i].GetName()):
            histos_data[i].Add(histos_SingleTop[i], -1)
            histos_data[i].SetDirectory(0)


    for i in range(0, len(histos_data)):
        if "MuMu" in str(histos_data[i].GetName()):
            if histos_data[i].Integral() != 0:
                print "ERROR: the same-lepton category in data has non-zero entries"
            for j in range(0, len(histos_data)):
                if str(histos_data[i].GetName()).replace("MuMu", "MuEl") == str(histos_data[j].GetName()):
                    name = histos_data[i].GetName()
                    if "_vs_" in str(histos_data[i]):
                        temp = cloneTH2(histos_data[i])
                    else:
                        temp = cloneTH1(histos_data[i])
                    temp.Add(histos_data[j], 1)
                    temp.Scale(0.5)
                    temp.SetName(name.replace("_clone", ""))
                    temp.SetDirectory(0)
                    est_ttbar.append(temp)
                    del temp 
        elif "ElEl" in str(histos_data[i].GetName()):
            if histos_data[i].Integral() != 0:
                print "ERROR: the same-lepton category in data has non-zero entries"
            for j in range(0, len(histos_data)):
                if str(histos_data[i].GetName()).replace("ElEl", "MuEl") == str(histos_data[j].GetName()):
                    name = histos_data[i].GetName()
                    if "_vs_" in str(histos_data[i]):
                        temp = cloneTH2(histos_data[i])
                    else:
                        temp = cloneTH1(histos_data[i])
                    temp.Add(histos_data[j], 1)
                    temp.Scale(0.5)
                    temp.SetName(name.replace("_clone", ""))
                    temp.SetDirectory(0)
                    est_ttbar.append(temp)
                    del temp 
        elif "MuEl" in str(histos_data[i].GetName()):
            name = histos_data[i].GetName()
            if "_vs_" in str(histos_data[i].GetName()):
                temp = cloneTH2(histos_data[i])
            else:
                temp = cloneTH1(histos_data[i])
            temp.Add(histos_data[i], 1)
            temp.SetName(name.replace("_clone", ""))
            temp.SetDirectory(0)
            est_ttbar.append(temp)
            del temp 

    print "Number of new histograms (should coincide with the total number of histograms): ", len(est_ttbar)
    # In PlotIt, these histograms will be normalized with the lumi. Since this is data,
    # we don't want any normalization by lumi. To avoid this, we "normalize" all the plots
    # by 1/lumi here.
    for h in est_ttbar:
        h.Scale(1/lumi)
        #print "h.GetName(): ", h.GetName(), "  h.Integral(): ", h.Integral(), "  h.GetEntries(): ", h.GetEntries()

 
    # Save the file that contains the ttbar estimation
    r_file = ROOT.TFile.Open("ttbar_from_data.root", "recreate")
    for hist in est_ttbar:
        hist.Write()
    r_file.Close()




#main
if __name__ == "__main__":
    ROOT.gROOT.SetBatch(True)
    main()