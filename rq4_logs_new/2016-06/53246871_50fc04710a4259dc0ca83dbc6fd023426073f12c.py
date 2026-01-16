from functions import*
from ROOT import *
from time import sleep
import array, os, sys, numpy, copy


File_tr=TFile.Open("../boosted_presel/trigger_objects.root", "R")
histo_efficiency=copy.copy(File_tr.Get("histo_efficiency"))
histo_efficiency_up=copy.copy(File_tr.Get("histo_efficiency_upper"))
histo_efficiency_down=copy.copy(File_tr.Get("histo_efficiency_lower"))
File_tr.Close()


def trigger_function(histo_eff,htJet30=700):
    result = histo_eff.GetBinContent(htJet30)
    return result

jet_pT_cut=30
jet_eta_cut=2.4
jet_btag_cut=0.185
bbtag_cut=0.9
tau21_cut=1.

def tree_preselection_and(file_string, isData=0, dataset="", cross_section=1.,systematic="NoSyst", bbtag_cut=0.9,jet_btag_cut=-2):
    cut1=0
    cut2=0
    cut3=0
    cut4=0
    cut5=0
    cut6=0
    
    
    print file_string, isData, dataset, cross_section,systematic
    try:
        myFile=TFile(file_string, "R")
        tree=myFile.Get("tree")
        tree.Print("*bbtag*")
    except:
        print "nofile"

    if isData==1:
        count=1
    else:
        histo_weight=myFile.Get("CountWeighted")
        print histo_weight, myFile
        count=histo_weight.GetBinContent(1)

    xsec=cross_section
    print xsec, "cross_section"    

    AnalysisFile=TFile("../presel/presel_counter_"+systematic+"_"+file_string.split("/")[-1], "recreate")
    count_histo=TH1F("cutflow", "cutflow",7,0,7 )
    MyTree = TTree("MyTree", "MyTree")
    
    #TTree variables
    fatjets_pt= vector('double')()
    fatjets_eta= vector('double')()
    fatjets_phi= vector('double')()
    fatjets_mass= vector('double')()
    fatjets_mprunedcorr= vector('double')()
    fatjets_bbtag= vector('double')()
    fatjets_tau21= vector('double')()
    jets_pt= vector('double')()
    jets_eta= vector('double')()
    jets_phi= vector('double')()
    jets_mass= vector('double')()
    jets_btag= vector('double')()
    jets_fatjet= vector('double')()
    CMVAweight= vector('double')()
    CMVAweightUp= vector('double')()
    CMVAweightDown= vector('double')()
    trigger1=array.array('f', [0.])
    trigger2=array.array('f', [0.])
    trigger3=array.array('f', [0.])
    trigger_pre=array.array('f', [0.])
    trigger_muon=array.array('f', [0.])
    Vtype=array.array('f', [0.])
    puWeight=array.array('f', [0.])
    puWeightUp=array.array('f', [0.])
    puWeightDown=array.array('f', [0.])
    norm=array.array('f', [0.])
    n_OkJets_byCSV=array.array('f', [0.])
    n_OkFj=array.array('f', [0.])
    evt=array.array('f', [0.])
    cross_section=array.array('f', [0.])
    b_evt=array.array('f', [-2.])
    r_evt=array.array('f', [-2.])
    htJet40eta3=array.array('f', [-100.0])
    trigWeight=array.array('f', [-100.0])
    MyTree.Branch("htJet40eta3", htJet40eta3, "htJet40eta3")
    MyTree.Branch("trigWeight", trigWeight, "trigWeight")
    #branches
    MyTree.Branch("fatjets_pt", fatjets_pt)
    MyTree.Branch("fatjets_eta", fatjets_eta)
    MyTree.Branch("fatjets_phi", fatjets_phi)
    MyTree.Branch("fatjets_mass", fatjets_mass)
    MyTree.Branch("fatjets_mprunedcorr", fatjets_mprunedcorr)
    MyTree.Branch("fatjets_bbtag", fatjets_bbtag)
    MyTree.Branch("fatjets_tau21", fatjets_tau21)
    MyTree.Branch("jets_pt", jets_pt)
    MyTree.Branch("jets_eta", jets_eta)
    MyTree.Branch("jets_phi", jets_phi)
    MyTree.Branch("jets_mass", jets_mass)
    MyTree.Branch("jets_btag", jets_btag)
    MyTree.Branch("jets_fatjet", jets_fatjet)
    MyTree.Branch("CMVAweight", CMVAweight)
    MyTree.Branch("CMVAweightUp", CMVAweightUp)
    MyTree.Branch("CMVAweightDown", CMVAweightDown)
    MyTree.Branch("Vtype", Vtype, "Vtype")
    MyTree.Branch("puWeight", puWeight, "puWeight")
    MyTree.Branch("puWeightUp", puWeightUp, "puWeightUp")
    MyTree.Branch("puWeightDown", puWeightDown, "puWeightDown")
    MyTree.Branch("norm", norm, "norm")
    MyTree.Branch("n_OkJets_byCSV", n_OkJets_byCSV, "n_OkJets_byCSV")
    MyTree.Branch("n_OkFj", n_OkFj, "n_OkFj")
    MyTree.Branch("evt", evt, "evt")
    MyTree.Branch("cross_section", cross_section, "cross_section")
    MyTree.Branch("boosted_evt", b_evt, "boosted_evt")
    MyTree.Branch("resolved_evt", r_evt, "resolved_evt")
    MyTree.Branch("HLT_ht800", trigger1, "HLT_ht800")
    MyTree.Branch("HLT_AK08", trigger2, "HLT_AK08")
    MyTree.Branch("HLT_HH4b", trigger3, "HLT_HH4b")
    MyTree.Branch("HLT_ht350", trigger_pre, "HLT_ht350")
    MyTree.Branch("HLT_mu20", trigger_muon, "HLT_mu20")
    entries=tree.GetEntries()
    for entry in range(tree.GetEntries()):
        tree.GetEntry(entry)
#        print entry, tree.evt, "cccc"
        if tree.isData:
            pWeight=1.
            pWeightUp=1.
            pWeightDown=1.
        else:
            pWeight=tree.puWeight
            pWeightUp=tree.puWeightUp
            pWeightDown=tree.puWeightDown
        weight=(xsec)*(pWeight)/count
        ht_=0
        for i in range(0,len(tree.Jet_pt)):
            if abs(tree.Jet_eta[i])<3 and tree.Jet_pt[i] >40 :
                ht_=ht_+tree.Jet_pt[i]
        htJet40eta3[0]=ht_
        
        trigWeight[0]=trigger_function(histo_efficiency,int(round(ht_)))
        #trigger_all= (tree.HLT_BIT_HLT_PFHT800_v or  tree.HLT_BIT_HLT_AK8DiPFJet250_200_TrimMass30_BTagCSV0p45_v or tree.HLT_HH4bHighLumi or tree.HLT_BIT_HLT_PFHT350_v or  tree.HLT_BIT_HLT_Mu20_v)
        trigger_all= (tree.HLT_BIT_HLT_PFHT800_v or  tree.HLT_BIT_HLT_AK8DiPFJet250_200_TrimMass30_BTagCSV0p45_v or tree.HLT_HH4bHighLumi)
        if file_string.startswith("../Data/BTag"):
            print "(tree.HLT_BIT_HLT_PFHT800_v==0 and  tree.HLT_BIT_HLT_AK8DiPFJet250_200_TrimMass30_BTagCSV0p45_v==0 and tree.HLT_HH4bHighLumi)"
#            quit()
            trigger_all= (tree.HLT_BIT_HLT_PFHT800_v==0 and  tree.HLT_BIT_HLT_AK8DiPFJet250_200_TrimMass30_BTagCSV0p45_v==0 and tree.HLT_HH4bHighLumi)
        if (tree.json_silver==1 and trigger_all):
            
            cut1=cut1+1
            if (tree.Vtype==-1 or tree.Vtype==4):
                cut2=cut2+1
                print tree.nJet, "getti adronici"
                try: 
                    F_vars=[tree.nFatjetAK08ungroomed, tree.FatjetAK08ungroomed_pt, tree.FatjetAK08ungroomed_eta, tree.FatjetAK08ungroomed_phi,
                tree.FatjetAK08ungroomed_mass, tree.FatjetAK08ungroomed_mprunedcorr, tree.FatjetAK08ungroomed_tau1, tree.FatjetAK08ungroomed_tau2,
                tree.FatjetAK08ungroomed_bbtag, tree.FatjetAK08ungroomed_mprunedcorr_UP, tree.FatjetAK08ungroomed_JEC_L1L2L3, tree.FatjetAK08ungroomed_JEC_UP, 
                tree.FatjetAK08ungroomed_JER_UP_PT ]
                except:
                    F_vars=[tree.nFatjetAK08ungroomed, tree.FatjetAK08ungroomed_pt, tree.FatjetAK08ungroomed_eta, tree.FatjetAK08ungroomed_phi,
                tree.FatjetAK08ungroomed_mass, tree.FatjetAK08ungroomed_mprunedcorr, tree.FatjetAK08ungroomed_tau1, tree.FatjetAK08ungroomed_tau2,
                tree.FatjetAK08ungroomed_bbtag]
                try:
                    Jet_vars=[tree.nJet, tree.Jet_pt, tree.Jet_eta, tree.Jet_phi, tree.Jet_mass, tree.Jet_btagCMVAV2, tree.Jet_corr_JECUp,
                tree.Jet_corr, tree.Jet_corr_JECDown, tree.Jet_corr_JERUp , tree.Jet_corr_JER, tree.Jet_corr_JERDown]
                except:
                    Jet_vars=[tree.nJet, tree.Jet_pt, tree.Jet_eta, tree.Jet_phi, tree.Jet_mass, tree.Jet_btagCMVAV2, tree.Jet_corr_JECUp,
                tree.Jet_corr, tree.Jet_corr_JECDown, tree.Jet_pt , tree.Jet_pt, tree.Jet_pt]
                function=sort_by_mass_save_candidates(F_vars, Jet_vars, systematic)
                count_jetsoutsidef=function[2]
                count_fatjets=function[3]
                jets_btag_array=function[1]
                save_index=function[0]
                if (count_fatjets>=1): cut3=cut3+1
                if (count_jetsoutsidef>=2 and count_fatjets>=1):
                    print "counted", entry
                    f_i=0
                    fatjets_pt.clear()
                    fatjets_eta.clear()
                    fatjets_phi.clear()
                    fatjets_mass.clear()
                    fatjets_mprunedcorr.clear()
                    fatjets_bbtag.clear()
                    fatjets_tau21.clear()
                    print save_index, "indici salvati"
                    for j in save_index:
                        correction_factor=tree.FatjetAK08ungroomed_mprunedcorr[j]/tree.FatjetAK08ungroomed_mpruned[j]
                        
                        fj_p4=TLorentzVector()                    
                        fj_p4.SetPtEtaPhiM(tree.FatjetAK08ungroomed_pt[j], tree.FatjetAK08ungroomed_eta[j], tree.FatjetAK08ungroomed_phi[j], tree.FatjetAK08ungroomed_mass[j])
                        if (systematic=="FJEC_Up"):
                            correction_factor=1+(F_vars[11][j]-F_vars[10][j])
                            fj_p4*=correction_factor
                        
                        if (systematic=="FJEC_Down"):
                            correction_factor=1-(F_vars[11][j]-F_vars[10][j])
                            fj_p4*=correction_factor
                            
                        if (systematic=="FJER_Up"):
                            correction_factor=div_except(F_vars[12][j],F_vars[1][j])
                            fj_p4*=correction_factor
                        
                        if (systematic=="FJER_Down"):
                            pJERDown=2*F_vars[1][j]-F_vars[12][j]
                            correction_factor=div_except((pJERDown),F_vars[1][j])
                            fj_p4*=correction_factor
                        fatjets_pt.push_back(fj_p4.Pt())
                        fatjets_eta.push_back(fj_p4.Eta())
                        fatjets_phi.push_back(fj_p4.Phi())
                        fatjets_mass.push_back(fj_p4.M())
                        mpruned_syst=tree.FatjetAK08ungroomed_mprunedcorr[j]
                        
                        if (systematic=="MJEC_Down"): 
                            sigma=tree.FatjetAK08ungroomed_JEC_L2L3_UP[j]-tree.FatjetAK08ungroomed_JEC_L2L3[j]
                            mpruned_syst=tree.FatjetAK08ungroomed_mpruned[j]*(tree.FatjetAK08ungroomed_JEC_L2L3[j]-sigma)
                        if (systematic=="MJEC_Up"): mpruned_syst=tree.FatjetAK08ungroomed_mpruned[j]*tree.FatjetAK08ungroomed_JEC_L2L3_UP[j]
                        fatjets_mprunedcorr.push_back(mpruned_syst)
                        fatjets_bbtag.push_back(tree.FatjetAK08ungroomed_bbtag[j])
                        if tree.FatjetAK08ungroomed_tau1[j]>0:
                            fatjets_tau21.push_back(tree.FatjetAK08ungroomed_tau2[j]/tree.FatjetAK08ungroomed_tau1[j])
                        else: fatjets_tau21.push_back(1.)
                        f_i=f_i+1
                        print "mpruned", tree.FatjetAK08ungroomed_mprunedcorr[j], "pt", tree.FatjetAK08ungroomed_pt[j], "index", j
                    jets_btag_array.view('f8,f8,f8,f8').sort(order=['f0'], axis=0)
                    jets_btag_array=jets_btag_array[::-1]
                    index_array2=jets_btag_array[:,1]
                    pT_array2=jets_btag_array[:,3]
                    pT_array2=pT_array2[(pT_array2 >= 0)]
                    b_array2=jets_btag_array[:,0]
                    b_array2=b_array2[(b_array2 > -1)]
                    index_array2=index_array2[(index_array2 >= 0)]
                    print "devo analizzare questo", index_array2
                    
                    print index_array2, pT_array2, b_array2
                    j_i=0
                    
                    jets_pt.clear()
                    jets_eta.clear()
                    jets_phi.clear()
                    jets_mass.clear()
                    jets_btag.clear()
                    jets_pt.clear()
                    jets_eta.clear()
                    jets_phi.clear()
                    jets_mass.clear()
                    jets_btag.clear()
                    jets_fatjet.clear()
                    CMVAweight.clear()
                    CMVAweightUp.clear()
                    CMVAweightDown.clear()
                    for j2 in index_array2:
                        
                        print j_i
                        j2=int(j2)
#                        
                        jets_pt.push_back(pT_array2[j_i])
                        jets_eta.push_back(tree.Jet_eta[j2])
                        jets_phi.push_back(tree.Jet_phi[j2])
                        jets_mass.push_back(tree.Jet_mass[j2])
                        jets_btag.push_back(tree.Jet_btagCMVAV2[j2])
                        jets_fatjet.push_back(jets_btag_array[j_i][2])
                        print "pt", pT_array2[j_i], "cmva", tree.Jet_btagCMVAV2[j2],jets_btag_array[j_i][0], "fjet_ref", jets_btag_array[j_i][2]
                        if isData==1:
                            CMVAweight.push_back(1)
                            CMVAweightUp.push_back(1)
                            CMVAweightDown.push_back(1)
                        else:
                            CMVAweight.push_back(tree.Jet_btagCMVAV2MSF[j2])
                            CMVAweightUp.push_back(tree.Jet_btagCMVAV2MSF_Up[j2])
                            CMVAweightDown.push_back(tree.Jet_btagCMVAV2MSF_Down[j2])

                        j_i=j_i+1
                    cross_section[0]=xsec
                    if tree.HLT_BIT_HLT_PFHT800_v: b_evt[0]=veto_boosted_event(F_vars)
                    else: b_evt[0]=0
                    if (tree.HLT_HH4bHighLumi and tree.Vtype==-1): r_evt[0]=veto_resolved_event(Jet_vars)
                    else: r_evt[0]=0
                    Vtype[0]=tree.Vtype
                    puWeight[0]=pWeight
                    puWeightUp[0]=pWeightUp
                    puWeightDown[0]=pWeightDown
                    norm[0]=count
                    n_OkJets_byCSV[0]=count_jetsoutsidef
                    n_OkFj[0]=count_fatjets
                    evt[0]=tree.evt
                    trigger1[0]=tree.HLT_BIT_HLT_PFHT800_v
                    trigger2[0]=tree.HLT_BIT_HLT_AK8DiPFJet250_200_TrimMass30_BTagCSV0p45_v
                    trigger3[0]=tree.HLT_HH4bHighLumi
                    trigger_pre[0]=tree.HLT_BIT_HLT_PFHT350_v
                    trigger_muon[0]=tree.HLT_BIT_HLT_Mu20_v
                 
#                    print len(jets_btag), "lenght"
#                    print jets_fatjet[0]
                    MyTree.Fill()
                    print MyTree.GetEntries()
                    
    
    MyTree.Write()
    count_histo.SetBinContent(1, count)
    count_histo.SetBinContent(2, cut1)
    count_histo.SetBinContent(3, cut2)
    count_histo.SetBinContent(4, cut3)
    count_histo.Write()
    print entries, MyTree.GetEntries(), "fraction",  MyTree.GetEntries()/float(entries)
    AnalysisFile.Close()
    myFile.Close()
                
                
