import calo_init
## add arguments relevant only for that script
calo_init.parser.add_argument("--windowSeed", help="Size of the window used for seeding [eta,phi]", type = int, nargs=2)
calo_init.parser.add_argument("--windowPos", help="Size of the window used for berycentre coalculation [eta,phi]", type = int, nargs=2)
calo_init.parser.add_argument("--windowDupl", help="Size of the window used for duplicate removal [eta,phi]", type = int, nargs=2)
calo_init.parser.add_argument("--dEta", help="Size of the tower in eta", type = float, nargs=2)
calo_init.parser.add_argument("--maxEta", help="Maximum eta", type = float)
group = calo_init.parser.add_mutually_exclusive_group()
group.add_argument("--dPhi", help="Size of the tower in phi", type = float)
group.add_argument("--numPhi", help="Number of the towers in phi", type = int)
calo_init.parser.add_argument("--zoom", help="How many bins around centre should be visible", type = int, nargs=2)
calo_init.parser.add_argument("--event", help="Number of an event to draw", type = int)
calo_init.parser.add_argument("inputSim", help="Input file name with the simulated event", type = str)
calo_init.parse_args()

from math import pi, floor
# set of default parameters
eventToDraw = 0
maxEta = 1.79
nPhi = 629 # artificially increase by 1 (odd number) - to make plots look OK
dEta = 0.01
nEta = int(2*maxEta/dEta + 1)
dPhi = 2*pi/nPhi
nameClusterCollection = "caloClusters"
nameParticlesCollection = "GenParticles"
zoomEta = 21
zoomPhi = 21
etaWindowSeed = 9
phiWindowSeed = 9
etaWindowPos = 3
phiWindowPos = 3
etaWindowDupl = 5
phiWindowDupl = 5
filenameSim = calo_init.args.inputSim
# get parameters if passed from command line
if calo_init.args.maxEta:
    maxEta = calo_init.args.maxEta
if calo_init.args.numPhi:
    nPhi = calo_init.args.numPhi
    dPhi = 2*pi/nPhi
if calo_init.args.dPhi:
    dPhi = calo_init.args.dPhi
    nPhi = int(dPhi*2*pi)
if calo_init.args.windowSeed:
    etaWindowSeed = calo_init.args.windowSeed[0]
    phiWindowSeed = calo_init.args.windowSeed[1]
if calo_init.args.windowPos:
    etaWindowPos = calo_init.args.windowPos[0]
    phiWindowPos = calo_init.args.windowPos[1]
if calo_init.args.windowDupl:
    etaWindowDupl = calo_init.args.windowDupl[0]
    phiWindowDupl = calo_init.args.windowDupl[1]
if calo_init.args.zoom:
    zoomEta = calo_init.args.zoom[0]
    zoomPhi = calo_init.args.zoom[1]
if calo_init.args.event:
    eventToDraw = calo_init.args.event

from ROOT import gSystem
gSystem.Load("libCaloAnalysis")
from ROOT import CaloAnalysis_recoMonitor, TCanvas, TFile, gStyle, gPad, kGreen, kRed, kBlue, TColor, TF1
from draw_functions import *

# use this script for multiple files
# gStyle.SetPalette(56) # kInvertedDarkBodyRadiator
gStyle.SetPalette(73) # kCMYK

for energy, filename in zip(calo_init.energies, calo_init.filenames):
    print "File with simulation results: "+filenameSim
    print "File with reconstruction results: "+filename
    analysis = CaloAnalysis_recoMonitor(nameClusterCollection,
                                        nameParticlesCollection,
                                        energy,
                                        maxEta, # max eta
                                        nEta, # number of bins in eta
                                        nPhi, # number of bins in phi
                                        dEta, # tower size in eta
                                        dPhi) # tower size in phi
    analysis.loop(filenameSim, filename,calo_init.verbose)
    # retrieve histograms to draw them
    histograms = analysis.histograms()
    hEn = histograms.hEn
    hEnFncPhi = histograms.hEnFncPhi
    hEta = histograms.hEta
    hPhi = histograms.hPhi
    hPhiFncPhi = histograms.hPhiFncPhi
    hEtaFncEta = histograms.hEtaFncEta
    hNo = histograms.hNo
    hNoFncPhi = histograms.hNoFncPhi
    hEnDiffMoreClu = histograms.hEnDiffMoreClu
    hEtaDiffMoreClu = histograms.hEtaDiffMoreClu
    hPhiDiffMoreClu = histograms.hPhiDiffMoreClu
    hRDiffMoreClu = histograms.hRDiffMoreClu
    h1d = [hEn, hEta, hPhi, hNo, hEnDiffMoreClu, hEtaDiffMoreClu, hPhiDiffMoreClu, hRDiffMoreClu]
    for h in h1d:
        h.SetMarkerColor(39)
        h.SetFillColor(39)
        h.SetLineColor(39)
        h.SetMarkerStyle(21)
        h.SetMarkerSize(1.8)

    # hist = []
    # hist.append(histograms.hAllCellEnergy)
    # hist.append(histograms.hClusterEnergy)
    # hist.append(histograms.hClusterCellEnergy)

    # ## Calculate some parameters
    # enTotal = hist[0].Integral()
    # meanEta = hist[0].ProjectionX().GetMean()
    # meanPhi = hist[0].ProjectionY().GetMean()

    # ## Set drawing options
    # for h in hist:
    #     h.GetYaxis().SetRangeUser(meanPhi - (floor(zoomPhi/2) - 0.5 ) * dPhi,meanPhi + (floor(zoomPhi/2) + 0.5 ) * dPhi)
    #     h.GetXaxis().SetRangeUser(meanEta - (floor(zoomEta/2) - 0.5 ) * dEta,meanEta + (floor(zoomEta/2) + 0.5 ) * dEta)
    #     h.GetXaxis().SetNdivisions(zoomEta)
    #     h.GetYaxis().SetNdivisions(zoomPhi)
    #     h.GetXaxis().SetLabelSize(0.02)
    #     h.GetYaxis().SetLabelSize(0.02)

    # canv = TCanvas('ECal_map_e'+str(energy)+'GeV', 'ECal', 1600, 1400 )
    # canv.Divide(2,2)
    # pad = canv.cd(1)
    # pad.SetGrid()
    # draw_hist2d(hist[0])
    # pad = canv.cd(2)
    # pad.SetLogz()
    # pad.SetGrid()
    # draw_hist2d(hist[0])
    # draw_text(["energy: "+str(round(enTotal,2))+" GeV"])

    # pad2 = canv.cd(3)
    # draw_hist2d(hist[1])
    # pad2.SetGrid()
    # draw_text(["energy: "+str(round(hist[1].Integral(),2))+" GeV",
    #            "           "+str(round(hist[1].Integral()/enTotal*100,1))+" %"])

    # pad = canv.cd(4)
    # pad.SetGrid()
    # draw_hist2d(hist[2])
    # draw_text(["energy: "+str(round(hist[2].Integral(),2))+" GeV",
    #            "           "+str(round(hist[2].Integral()/enTotal*100,1))+" %"])
    # for ipad in range(2,5):
    #     canv.cd(ipad)
    #     draw_rectangle([meanEta-etaWindowSeed/2.*dEta, meanPhi-phiWindowSeed/2.*dPhi],
    #                    [meanEta+etaWindowSeed/2.*dEta, meanPhi+phiWindowSeed/2.*dPhi], kRed, 4)
    #     draw_rectangle([meanEta-etaWindowDupl/2.*dEta, meanPhi-phiWindowDupl/2.*dPhi],
    #                    [meanEta+etaWindowDupl/2.*dEta, meanPhi+phiWindowDupl/2.*dPhi], kBlue, 3)
    #     draw_rectangle([meanEta-etaWindowPos/2.*dEta, meanPhi-phiWindowPos/2.*dPhi],
    #                    [meanEta+etaWindowPos/2.*dEta, meanPhi+phiWindowPos/2.*dPhi], kGreen, 2)

    # canv.Print('ECal_map_e'+str(energy)+'GeV.png')

en = 50
canv = TCanvas( 'canv', 'ECal', 1900, 1200 )
canv.Divide(4,3)
canv.cd(1)

fit = TF1('fit','gaus',0.8*en,1.2*en)
res = hEn.Fit('fit','S')
fit2 = TF1('fit2','gaus',res.Get().Parameter(1)-2.*res.Get().Parameter(2),res.Get().Parameter(1)+2.*res.Get().Parameter(2))
res2 = hEn.Fit('fit2','SR')
fitEta = TF1('fitEta','gaus',-10*dEta,10*dEta)
resEta = hEta.Fit('fitEta','S')
fitPhi = TF1('fitPhi','gaus',-10*dPhi,10*dPhi)
resPhi = hPhi.Fit('fitPhi','S')


hEn.SetDrawOption('bar')
hEnDiffMoreClu.SetDrawOption('bar')
hNo.SetDrawOption('text')
hEta.SetDrawOption('bar')
hPhi.SetDrawOption('bar')

hEn.GetXaxis().SetTitle('energy (GeV)')
hEn.GetYaxis().SetTitle('fraction of events')
hEn.GetYaxis().SetTitleOffset(1.2)

hEnDiffMoreClu.GetXaxis().SetTitle('#Delta E / E')
hEnDiffMoreClu.GetYaxis().SetTitle('number of events')
hEtaDiffMoreClu.GetXaxis().SetTitle('#Delta#eta')
hEtaDiffMoreClu.GetYaxis().SetTitle('number of events')
hPhiDiffMoreClu.GetXaxis().SetTitle('#Delta#varphi')
hPhiDiffMoreClu.GetYaxis().SetTitle('number of events')
hRDiffMoreClu.GetXaxis().SetTitle('#Delta R')
hRDiffMoreClu.GetYaxis().SetTitle('number of events')
hEnDiffMoreClu.GetYaxis().SetTitleOffset(1.2)
hNo.GetYaxis().SetTitle('fraction of events')
hNo.GetXaxis().SetTitle('number of clusters per event')
hEta.GetXaxis().SetTitle('#Delta#eta')
hEta.GetYaxis().SetTitle('fraction of events')
hPhi.GetXaxis().SetTitle('#Delta#varphi')
hPhi.GetYaxis().SetTitle('fraction of events')


hNo.GetXaxis().SetNdivisions(5)


# hNo.GetYaxis().SetRangeUser(0,1.1)
hEta.GetXaxis().SetRangeUser(-dEta,dEta)
hEtaFncEta.GetXaxis().SetRangeUser(-0.1,0.1)
hEtaFncEta.GetYaxis().SetRangeUser(-dEta,dEta)
hPhi.GetXaxis().SetRangeUser(-2*dPhi,2*dPhi)
hPhiFncPhi.GetYaxis().SetRangeUser(-2*dPhi,2*dPhi)


canv.cd(1)
gStyle.SetOptFit(1)
hEn.Draw('bar')
draw_text(["energy: "+str(round(res2.Get().Parameter(1),1))+" GeV",
           "           "+str(round(res2.Get().Parameter(1),1)/en*100.)+" %",
           "resolution: "+str(round(res2.Get().Parameter(2)/res2.Get().Parameter(1)*100,1))+" %"], [0.6,0.2,0.9,0.4])
canv.cd(2)
draw_hist2d(hEnFncPhi, '#varphi', 'energy (GeV)')
canv.cd(3)
hNo.Draw()
hNo.Draw('text same')
canv.cd(4)
draw_hist2d(hNoFncPhi, '#varphi', 'number of clusters per event')
canv.cd(5)
hEnDiffMoreClu.Draw()
canv.cd(6)
hEtaDiffMoreClu.Draw()
canv.cd(7)
hPhiDiffMoreClu.Draw()
canv.cd(8)
hRDiffMoreClu.Draw()
canv.cd(9)
hEta.Draw('bar')
canv.cd(10)
draw_hist2d(hEtaFncEta, '#eta','#Delta#eta')
canv.cd(11)
hPhi.Draw('bar')
canv.cd(12)
draw_hist2d(hPhiFncPhi, '#varphi','#Delta#varphi')

plots = TFile("Energy_reco_plots.root","RECREATE")
plots.cd()
hEn.Write()
hNo.Write()
hEta.Write()
hPhi.Write()
hEnFncPhi.Write()
hEnDiffMoreClu.Write()
hEtaDiffMoreClu.Write()
hPhiDiffMoreClu.Write()
hRDiffMoreClu.Write()
hNoFncPhi.Write()
hPhiFncPhi.Write()
hEtaFncEta.Write()
plots.Close()

if calo_init.filenameOut:
    canv.Print(calo_init.filenameOut+".png")
else:
    canv.Print("Energy_monitor.png")