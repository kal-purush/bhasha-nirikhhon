import os
parts = [3, 10, 30, 100]
runs = [
"33362", #sept 7 dedicated running
#"33478", #10 pA run
"33498"  #longer 10 pA run
]

jobs = []

for part in parts:
    for run in runs:
          cmd = "python3.10 beamCurrents.py -r "+str(run)+" -e 1500  -n "+str(part)
          print(cmd)
          jobs.append(cmd)
       #   os.system(cmd)


import ROOT as r
import pulseAnalysis as p
import copy
nCuts = 6

suffixes = [
"",
"BG",
"E",
"EBG"
]#symbols

sigmaGraphs = []
poissonGraphs = []

sigmaGraphsEnergy = []
poissonGraphsEnergy = []
#compare 2 graphs per cut that should go as the sqrt(N) and be equal
#do for N, THEN energy
#then add background subtracted runs
#make these all gifs so you can go through and explain them in slides 1-by-1

can = r.TCanvas()
can.SetLogy(1)
#for run in runs: #graphs
for i in range(nCuts): #colors
    sigmaGraphs.append(copy.deepcopy( r.TGraphErrors()) )
    poissonGraphs.append(copy.deepcopy( r.TGraphErrors()) )
    for part in parts: # points
        filename = "pulseCountingRun"+str(run)+"_n"+str(part)+".root"
        f = r.TFile.Open("./"+filename,"READ")
        t = f.Get("rates")
  #      g = f.Get("g"+suffix(i)+str(i))
       
        hSignal = r.TH1F("hSignal", "", 100, 0, 1000)
        hPoisson = r.TH1F("hPoisson","", 100, 0, 1000 )
        for e in t:
            signalRate = t.signalRate[i]
            poissonErr = t.poissonErr[i]
            signal_BGRate = t.signal_BGRate[i]
            poissonErrBGRate = t.signal_BGRate[i]
            hSignal.Fill(signalRate)
            hPoisson.Fill(poissonErr)

        
        sigmaGraphs[i].AddPoint(part,  hSignal.GetRMS() )
        poissonGraphs[i].AddPoint(part, hPoisson.GetMean() )
    
    sigmaGraphs[i].SetMarkerColor(p.colors[i])
    sigmaGraphs[i].SetLineColor(p.colors[i])

    sigmaGraphs[i].SetMarkerStyle(23)
    poissonGraphs[i].SetMarkerColor(p.colors[i])
    poissonGraphs[i].SetLineColor(p.colors[i])
    poissonGraphs[i].SetMarkerStyle(32)
    poissonGraphs[i].SetLineStyle(8)
    if i ==0:
        sigmaGraphs[i].SetTitle("Comparison of Sample Std Dev to Poisson Errors;Number of partitions of run;Estimated Error [Hz];")
        sigmaGraphs[i].SetMinimum(0.1)
        sigmaGraphs[i].Draw("APL")
    else:
       sigmaGraphs[i].Draw("same&&PL")
    poissonGraphs[i].Draw("PL&&same")
    p.savePlots(can, "./", "systematicsCheck_cut"+str(i) )
    i+=1
     #   if suffix == "" or suffix == "E"
      #      sigmaGraphs[i].SetPointError(index?, 0, g.Get)
       # if suffix == "BG" or suffix == "EBG"
       #     sigmaGraphs[i].append( g.GetRMS(2) )
                
            #std dev  == mean (poissonErrs)?
            
            #plot the std dev and mean of poisson errors as a fcn of n
            # fit to sqrt(N)


print("the run/completed jobs:")
for job in jobs:
    print(job)

