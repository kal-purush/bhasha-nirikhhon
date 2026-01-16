#!usr/bin/python
import ROOT
from ROOT import TFile, TH1, TH1F,TLine,   TCanvas, TProfile, TEfficiency, TGraphAsymmErrors, TF1, TPaveText, TPaveStats, TLegend, TLine, gROOT, gPad, gStyle
from tdrstyle  import tdrStyle
from array import array

gROOT.Reset()
tdrStyle()
gROOT.SetStyle('tdrStyle')
gROOT.ForceStyle()

#import optparse
#usage = "usage: %prog [options]"
#parser = optparse.OptionParser(usage)
#parser.add_option("--bit"  ,action="store",type="string",dest="bit",default='pfCor40')
#parser.add_option("--var"  ,action="store",type="string",dest="var",default='Inclusive')

#(options, args) = parser.parse_args()

#bit = options.bit
#var = options.var

COLOR = [ROOT.kRed,ROOT.kBlue]
STYLE = [21,22]

TAGS = ["baseline", "regression"  ]

canPt = TCanvas('ResVsPt','ResVsPt_')

resPt= []
gEffPt = []
fit = []
ln = []
k = 0



for tag in TAGS:
  inf      = TFile.Open('/afs/cern.ch/work/d/degrutto/public/MiniAOD/ZnnHbb_Phys14_PU20bx25/skimV11/step3/Step3_ZnnH125.root')
  tree     = inf.Get('tree')
  hRes   = TH1F('hRes_'+tag,'hRes_'+tag,20,0,200)
  if tag == 'baseline':
#    tree.Draw('genPt>>hPtAll_'+tag, 'genPt>5')
#    tree.Draw('genPt>>hPtPass_'+tag,'pass && genMatchDR<0.2')
    tree.Draw('HCSV_mass >>hRes_'+tag, "",  "" )
  else:
    tree.Draw('HmassReg >>hRes_'+tag, "",  "" )
  resPt.append(hRes)
  resPt[k].SetMarkerColor(COLOR[k])
  resPt[k].SetLineColor(COLOR[k])
  resPt[k].SetMarkerStyle(STYLE[k]) 

  x   = []
  y   = []
  exl = []
  exh = []
  eyl = []
  eyh = []

  for i in xrange(hRes.GetNbinsX()):
    print tag
    print hRes.GetBinCenter(i+1), hRes.GetBinContent(i+1), hRes.GetBinError(i+1)
    x.append(hRes.GetBinCenter(i+1))
    exl.append(0) 
    exh.append(0) 
    if hRes.GetBinContent(i+1)>0 :
      # resolution as sigma(hlt/gen) / <hlt/gen>
      y.append( hRes.GetBinError(i+1) / hRes.GetBinContent(i+1))
    else:
      y.append(0)
    eyl.append(0)
    eyh.append(0)

  vx = array('d',x)
  vy = array('d',y)
  vexl = array('d',exl)
  vexh = array('d',exh)
  veyl = array('d',eyl)
  veyh = array('d',eyh)

  gEffPt.append(TGraphAsymmErrors(hRes.GetNbinsX(),vx,vy,vexl,vexh,veyl,veyh))

  gEffPt[k].SetMarkerColor(COLOR[k])
  gEffPt[k].SetLineColor(COLOR[k])
  gEffPt[k].SetMarkerStyle(STYLE[k]) 

 # fit.append(TF1('fit'+str(k),'([0]-[3])/pow(1+[1]*exp(-[2]*x),[4])+[3]+[5]*(1-pow(x,-[6]))',15,200))
#  fit.append(TF1('fit'+str(k),'([0]-[3])/pow(1+[1]*exp(-[2]*x),[4])+[3]+[5]*(1-pow(x,-[6]))',0,300))
  fit.append(TF1('fit'+str(k),'gaus',80,160))
#  fit[k].SetParameters(1,300,0.1,0,1,1,0.1)
#  fit[k].SetParNames('A','x0','#mean')
# fit[k].SetParNames('B','x0','#sigma')
  fit[k].SetLineColor(COLOR[k])

  k+=1

canPt.cd()
#h = TH1F('h','h',50,0,100)
f = TLine( 125, 0 ,125,10000000)
f.SetLineColor(ROOT.kBlack)
f.SetLineStyle(3)
f.SetLineWidth(1)
f.Draw()

leg = TLegend(0.7,0.2,0.9,0.45)
#leg.SetHeader(bit)
#gEffPt[0].Draw('PE1')

for k in range(len(TAGS)):
#  x99 = fit[k].GetX(0.99)
#  x10 = fit[k].GetX(0.1)
#  x50 = fit[k].GetX(0.5)

##  print 'Turn-on: '+str(x90)
##  q = (x90-x10)/x50
##  print 'Sharpness: '+str(q)
#  gEffPt[k].Draw('samePE1)'
#  gEffPt[k].Draw('samePE')
 #resPt[k].Draw('samePE')
  resPt[k].Draw('same')
  resPt[k].GetXaxis().SetTitle('mass (GeV)')
  resPt[k].Fit(fit[k],'RNQ')
  fit[k].Draw('same')
  

 ## leg.AddEntry(gEffPt[k],TAGS[k]+' ('+'%.2f' % q +')','P')
  leg.AddEntry(gEffPt[k],TAGS[k],'P')
  par = fit[k].GetParameters()
#  print 'fit results: const =',par[0],',mean =',par[1], ',sigma =',par[2] 
  ln.append(TPaveText(0.28, 0.86 - 0.10 * k ,0.38 ,0.96 - 0.10 *k, "brNDC"))
  ln[k].SetTextColor(COLOR[k])
#  ln[k].SetLineStyle(7)
#  ln[k].SetLineWidth(2)
  ln[k].SetLineColor(0)
  ln[k].SetFillColor(0);
  ln[k].SetShadowColor(0);
  ln[k].SetBorderSize(1);
  ln[k].SetTextFont(62);
  ln[k].SetTextSize(0.03);
  ln[k].AddText('#mu '+str(round(par[1],2))+', #sigma '+str(round(par[2],2))+', res=#sigma/#mu '+str(round(par[2]/par[1],3)))
  ln[k].Draw("same")


leg.SetFillColor(0)
#leg.SetBorderSize(0)
leg.SetTextFont(62)
leg.SetTextSize(0.05)
leg.Draw()

gPad.Update()

#----- keep the GUI alive ------------
if __name__ == '__main__':
  rep = ''
  while not rep in ['q','Q']:
    rep = raw_input('enter "q" to quit: ')
    if 1 < len(rep):
      rep = rep[0]