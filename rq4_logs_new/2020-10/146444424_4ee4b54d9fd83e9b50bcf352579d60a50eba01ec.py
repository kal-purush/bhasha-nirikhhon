import logging, re

from hhAnalysis.multilepton.configs.analyzeConfig_hh import *
from tthAnalysis.HiggsToTauTau.jobTools import create_if_not_exists
from tthAnalysis.HiggsToTauTau.analysisTools import initDict, getKey, create_cfg, createFile, generateInputFileList
import sys

def get_lepton_selection_and_frWeight(lepton_selection, lepton_frWeight):
  lepton_selection_and_frWeight = lepton_selection 
  if lepton_selection.startswith("Fakeable"):
    if lepton_frWeight == "enabled":
      lepton_selection_and_frWeight += "_wFakeRateWeights"
    elif lepton_frWeight == "disabled":
      lepton_selection_and_frWeight += "_woFakeRateWeights"
  lepton_selection_and_frWeight = lepton_selection_and_frWeight.replace("|", "_")
  return lepton_selection_and_frWeight

def getHistogramDir(category, lepton_selection, lepton_frWeight, leptonChargeSelection):
  histogramDir = category
  if leptonChargeSelection != "disabled":
    histogramDir += "_%s" % leptonChargeSelection
  histogramDir += "_%s" % lepton_selection
  if lepton_selection.find("Fakeable") != -1:
    if lepton_frWeight == "enabled":
      histogramDir += "_wFakeRateWeights"
    elif lepton_frWeight == "disabled":
      histogramDir += "_woFakeRateWeights"
  return histogramDir

class analyzeConfig_WZctrl_SFstudy(analyzeConfig_hh):
  """Configuration metadata needed to run analysis in a single go.

  Sets up a folder structure by defining full path names; no directory creation is delegated here.

  Args specific to analyzeConfig_WZctrl_SFstudy:
    lepton_selection: either `Tight`, `Loose` or `Fakeable`

  See $CMSSW_BASE/src/tthAnalysis/HiggsToTauTau/python/analyzeConfig.py
  for documentation of further Args. 

  """
  def __init__(self,
        configDir,
        outputDir,
        executable_analyze,
        cfgFile_analyze,
        samples,
        hadTauVeto_selection,
        applyFakeRateWeights,
        leptonChargeSelections,
        central_or_shifts,
        lep_mva_wp,
        lep_useDifferentMVACutForLepton3,
        lep_mva_cut_mu_forLepton3,
        lep_mva_cut_e_forLepton3, 
        jet_cleaning_by_index,
        gen_matching_by_index,
        max_files_per_job,
        era,
        use_lumi,
        lumi,
        check_output_files,
        running_method,
        num_parallel_jobs,
        executable_addBackgrounds,
        executable_addBackgroundJetToTauFakes,
        histograms_to_fit,
        select_rle_output         = False,
        executable_prep_dcard     = "prepareDatacards",
        executable_add_syst_dcard = "addSystDatacards",
        select_root_output        = False,
        do_sync                   = False,
        verbose                   = False,
        dry_run                   = False,
        isDebug                   = False,
        rle_select                = '',
        use_nonnominal            = False,
        hlt_filter                = False,
        use_home                  = False,
        submission_cmd            = None,
      ):
    analyzeConfig_hh.__init__(self,
      configDir                 = configDir,
      outputDir                 = outputDir,
      executable_analyze        = executable_analyze,
      channel                   = "WZctrl_SFstudy",
      samples                   = samples,
      jet_cleaning_by_index     = jet_cleaning_by_index,
      gen_matching_by_index     = gen_matching_by_index,
      central_or_shifts         = central_or_shifts,
      max_files_per_job         = max_files_per_job,
      era                       = era,
      use_lumi                  = use_lumi,
      lumi                      = lumi,
      check_output_files        = check_output_files,
      running_method            = running_method,
      num_parallel_jobs         = num_parallel_jobs,
      histograms_to_fit         = histograms_to_fit,
      triggers                  = [ '1e', '1mu', '2e', '2mu', '1e1mu', '3e', '3mu', '1e2mu', '2e1mu' ],
      lep_mva_wp                = lep_mva_wp,
      lep_useDifferentMVACutForLepton3 = lep_useDifferentMVACutForLepton3,
      lep_mva_cut_mu_forLepton3        = lep_mva_cut_mu_forLepton3,
      lep_mva_cut_e_forLepton3         = lep_mva_cut_e_forLepton3,                              
      executable_prep_dcard     = executable_prep_dcard,
      executable_add_syst_dcard = executable_add_syst_dcard,
      do_sync                   = do_sync,
      verbose                   = verbose,
      dry_run                   = dry_run,
      isDebug                   = isDebug,
      use_home                  = use_home,
      template_dir              = os.path.join(os.getenv('CMSSW_BASE'), 'src', 'hhAnalysis', 'multilepton', 'test', 'templates'),
      submission_cmd            = submission_cmd,
      use_dymumu_tau_fr         = True,
      apply_nc_correction       = False,
    )
    self.lepton_selections = [ "Tight", "Fakeable" ]
    self.lepton_frWeights = [ "enabled", "disabled" ]
    self.hadTauVeto_selection_part2 = hadTauVeto_selection
    self.applyFakeRateWeights = applyFakeRateWeights

    self.apply_leptonGenMatching = None
    if applyFakeRateWeights == "3lepton":
      self.apply_leptonGenMatching = True
      if self.run_mcClosure:
        self.lepton_selections.extend([ "Fakeable_mcClosure_e", "Fakeable_mcClosure_m" ])
      self.central_or_shifts_fr = systematics.FRe_shape + systematics.FRm_shape
    else:
      raise ValueError("Invalid Configuration parameter 'applyFakeRateWeights' = %s !!" % applyFakeRateWeights)
    self.pruneSystematics()
    self.internalizeSystematics()

    '''
    print "analyzeConfig_WZctrl_SFstudy::__init__: lepton_genMatches_nonfakes:",self.lepton_genMatches_nonfakes,\
        ", lepton_genMatches_Convs:",self.lepton_genMatches_Convs,", lepton_genMatches_fakes:",self.lepton_genMatches_fakes;
    sys.stdout.flush();
    '''  

    self.leptonChargeSelections = leptonChargeSelections

    self.executable_addBackgrounds = executable_addBackgrounds
    self.executable_addFakes = executable_addBackgroundJetToTauFakes

    #self.nonfake_backgrounds = [ "TTH", "TTZ", "TTW", "TTWW", "TT", "DY", "W", "WW", "WZ", "ZZ", "VH", "TH"]
    self.nonfake_backgrounds = [ "ZZ", "WZ", "WW", "TT", "TTW", "TTWW", "TTZ", "DY", "W", "Other", "VH", "TTH", "TH", "ggH", "qqH", "TTWH", "TTZH"]

    self.cfgFile_analyze = os.path.join(self.template_dir, cfgFile_analyze)
    self.prep_dcard_processesToCopy = [ "data_obs" ] + self.nonfake_backgrounds + [ "Convs", "data_fakes", "fakes_mc" ] + self.get_samples_categories_HH()
    self.prep_dcard_signals = []
    for sample_name, sample_info in self.samples.items():
      if not sample_info["use_it"]:
        continue
      sample_category = sample_info["sample_category"]
      if sample_category.startswith("signal"):
        self.prep_dcard_signals.append(sample_category)

    self.histogramDir_prep_dcard = "WZctrl_SFstudy_OS_Tight"
    self.histogramDir_prep_dcard_SS = "WZctrl_SFstudy_SS_Tight"
    #self.make_plots_backgrounds = [ "TTH", "TTZ", "TTW", "TTWW", "TT", "DY", "W", "WW", "WZ", "ZZ", "VH", "TH"] + [ "Convs", "data_fakes"]
    self.make_plots_backgrounds = [ "ZZ", "WZ", "WW", "TT", "TTW", "TTWW", "TTZ", "DY", "W", "Other", "VH", "TTH", "TH", "ggH", "qqH", "TTWH", "TTZH"] + [ "Convs", "data_fakes" ]

    self.cfgFile_make_plots = os.path.join(self.template_dir, "makePlots_WZctrl_SFstudy_cfg.py")
    self.cfgFile_make_plots_mcClosure = os.path.join(self.template_dir, "makePlots_mcClosure_WZctrl_SFstudy_cfg.py") #TODO

    self.select_rle_output = select_rle_output
    self.select_root_output = select_root_output
    self.rle_select = rle_select
    self.use_nonnominal = use_nonnominal
    self.hlt_filter = hlt_filter

    self.isBDTtraining = False

    self.categories = [
      "WZctrl_3e", "WZctrl_3mu", "WZctrl_2e1mu", "WZctrl_1e2mu",
      "WZctrl_2lSFOS_plus_mu", "WZctrl_2lSFOS_plus_e",
      "WZctrl_3l_woLeptonSF",
      "WZctrl_3e_woLeptonSF", "WZctrl_3mu_woLeptonSF", "WZctrl_2e1mu_woLeptonSF", "WZctrl_1e2mu_woLeptonSF",
      "WZctrl_2lSFOS_plus_mu_woLeptonSF", "WZctrl_2lSFOS_plus_e_woLeptonSF",      
      "WZctrl_3l_woLeptonSFCor",
      "WZctrl_3e_woLeptonSFCor", "WZctrl_3mu_woLeptonSFCor", "WZctrl_2e1mu_woLeptonSFCor", "WZctrl_1e2mu_woLeptonSFCor",
      "WZctrl_2lSFOS_plus_mu_woLeptonSFCor", "WZctrl_2lSFOS_plus_e_woLeptonSFCor",      
    ]

    self.category_inclusive = "WZctrl_3l"
    if self.category_inclusive not in self.categories:
      self.categories.append(self.category_inclusive)

    self.lep_mva_cut_mu_forLepton3 = lep_mva_cut_mu_forLepton3
    self.lep_mva_cut_e_forLepton3  = lep_mva_cut_e_forLepton3

  def set_BDT_training(self):
    """Run analysis with loose selection criteria for leptons,
       for the purpose of preparing event list files for BDT training.
    """
    self.lepton_selections   = [ "forBDTtraining" ]
    self.lepton_frWeights    = [ "disabled" ]
    self.leptonChargeSelections = [ "OS" ]
    self.isBDTtraining       = True

  def accept_systematics(self, central_or_shift, is_mc, lepton_selection, leptonChargeSelection, sample_info):
    if central_or_shift != "central":
      isFR_shape_shift = (central_or_shift in self.central_or_shifts_fr)
      if not ((lepton_selection == "Fakeable" and isFR_shape_shift) or lepton_selection == "Tight"):
        return False
      if not is_mc and not isFR_shape_shift:
        return False
      if isFR_shape_shift and lepton_selection == "Tight":
        return False
      if not self.accept_central_or_shift(central_or_shift, sample_info):
        return False
    return True

  def createCfg_analyze(self, jobOptions, sample_info, lepton_selection):
    """Create python configuration file for the analyze_WZctrl_SFstudy executable (analysis code)

    Args:
      inputFiles: list of input files (Ntuples)
      outputFile: output file of the job -- a ROOT file containing histogram
      process: either `TT`, `TTW`, `TTZ`, `EWK`, `Rares`, `data_obs`, `ttH_hww`, 'ttH_hzg', 'ttH_hmm', `ttH_hzz` or `ttH_htt`
      is_mc: flag indicating whether job runs on MC (True) or data (False)
      lumi_scale: event weight (= xsection * luminosity / number of events)
      central_or_shift: either 'central' or one of the systematic uncertainties defined in $CMSSW_BASE/src/hhAnalysis/multilepton/bin/analyze_WZctrl_SFstudy.cc
    """
    lepton_frWeight = "disabled" if jobOptions['applyFakeRateWeights'] == "disabled" else "enabled"
    jobOptions['histogramDir'] = getHistogramDir(self.category_inclusive, lepton_selection, lepton_frWeight, jobOptions['leptonChargeSelection'])
    if 'mcClosure' in lepton_selection:
      self.mcClosure_dir['%s_%s' % (lepton_selection, jobOptions['leptonChargeSelection'])] = jobOptions['histogramDir']

    self.set_leptonFakeRateWeightHistogramNames(jobOptions['central_or_shift'], lepton_selection)
    ## Original
    jobOptions['leptonFakeRateWeight.inputFileName'] = self.leptonFakeRateWeight_inputFile  
    jobOptions['leptonFakeRateWeight.histogramName_e'] = self.leptonFakeRateWeight_histogramName_e
    jobOptions['leptonFakeRateWeight.histogramName_mu'] = self.leptonFakeRateWeight_histogramName_mu

    if "default" not in self.lep_mva_cut_mu_forLepton3:
      jobOptions['lep_mva_cut_mu_forLepton3'] = float(self.lep_mva_cut_mu_forLepton3)
    if "default" not in self.lep_mva_cut_e_forLepton3:
      jobOptions['lep_mva_cut_e_forLepton3']  = float(self.lep_mva_cut_e_forLepton3)

    lines = super(analyzeConfig_WZctrl_SFstudy, self).createCfg_analyze(jobOptions, sample_info)
    create_cfg(self.cfgFile_analyze, jobOptions['cfgFile_modified'], lines)

  def create(self):
    """Creates all necessary config files and runs the complete analysis workfow -- either locally or on the batch system
    """
    for sample_name, sample_info in self.samples.items():
      if not sample_info["use_it"]:
        continue

      sample_category = sample_info["sample_category"]
      is_mc = (sample_info["type"] == "mc")
      process_name = sample_info["process_name_specific"]

      logging.info("Building dictionaries for sample %s..." % process_name)
      for lepton_selection in self.lepton_selections:
        for lepton_frWeight in self.lepton_frWeights:
          if lepton_frWeight == "enabled" and not lepton_selection.startswith("Fakeable"):
            continue
          if lepton_frWeight == "disabled" and not lepton_selection in [ "Tight", "forBDTtraining" ]:
            continue

          lepton_selection_and_frWeight = get_lepton_selection_and_frWeight(lepton_selection, lepton_frWeight)
          for leptonChargeSelection in self.leptonChargeSelections:
            central_or_shift_extensions = ["", "hadd", "addBackgrounds"]
            central_or_shift_dedicated = self.central_or_shifts if self.runTHweights(sample_info) else self.central_or_shifts_external
            central_or_shifts_extended = central_or_shift_extensions + central_or_shift_dedicated
            for central_or_shift_or_dummy in central_or_shifts_extended:
              process_name_extended = [ process_name, "hadd" ]
              for process_name_or_dummy in process_name_extended:
                if central_or_shift_or_dummy in [ "hadd", "addBackgrounds" ] and process_name_or_dummy in [ "hadd" ]:
                  continue

                if central_or_shift_or_dummy not in central_or_shift_extensions and not self.accept_systematics(
                    central_or_shift_or_dummy, is_mc, lepton_selection, leptonChargeSelection, sample_info
                ):
                  continue

                key_dir = getKey(process_name_or_dummy, lepton_selection_and_frWeight, leptonChargeSelection, central_or_shift_or_dummy)
                for dir_type in [ DKEY_CFGS, DKEY_HIST, DKEY_LOGS, DKEY_RLES, DKEY_SYNC ]:
                  if dir_type == DKEY_SYNC and not self.do_sync:
                    continue
                  initDict(self.dirs, [ key_dir, dir_type ])
                  if dir_type in [ DKEY_CFGS, DKEY_LOGS ]:
                    self.dirs[key_dir][dir_type] = os.path.join(self.configDir, dir_type, self.channel,
                      "_".join([ lepton_selection_and_frWeight, leptonChargeSelection ]), process_name_or_dummy, central_or_shift_or_dummy)
                  else:
                    self.dirs[key_dir][dir_type] = os.path.join(self.outputDir, dir_type, self.channel,
                      "_".join([ lepton_selection_and_frWeight, leptonChargeSelection ]), process_name_or_dummy)
    for subdirectory in [ "addBackgrounds", "addBackgroundLeptonFakes", "prepareDatacards", "addSystFakeRates", "makePlots" ]:
      key_dir = getKey(subdirectory)
      for dir_type in [ DKEY_CFGS, DKEY_HIST, DKEY_LOGS, DKEY_DCRD, DKEY_PLOT ]:
        initDict(self.dirs, [ key_dir, dir_type ])
        if dir_type in [ DKEY_CFGS, DKEY_LOGS, DKEY_DCRD, DKEY_PLOT ]:
          self.dirs[key_dir][dir_type] = os.path.join(self.configDir, dir_type, self.channel, subdirectory)
        else:
          self.dirs[key_dir][dir_type] = os.path.join(self.outputDir, dir_type, self.channel, subdirectory)                    
    for dir_type in [ DKEY_CFGS, DKEY_SCRIPTS, DKEY_HIST, DKEY_LOGS, DKEY_DCRD, DKEY_PLOT, DKEY_HADD_RT, DKEY_SYNC ]:
      if dir_type == DKEY_SYNC and not self.do_sync:
        continue
      initDict(self.dirs, [ dir_type ])
      if dir_type in [ DKEY_CFGS, DKEY_SCRIPTS, DKEY_LOGS, DKEY_DCRD, DKEY_PLOT, DKEY_HADD_RT ]:
        self.dirs[dir_type] = os.path.join(self.configDir, dir_type, self.channel)
      else:
        self.dirs[dir_type] = os.path.join(self.outputDir, dir_type, self.channel)

    numDirectories = 0
    for key in self.dirs.keys():
      if type(self.dirs[key]) == dict:
        numDirectories += len(self.dirs[key])
      else:
        numDirectories += 1
    logging.info("Creating directory structure (numDirectories = %i)" % numDirectories)
    numDirectories_created = 0;
    frac = 1
    for key in self.dirs.keys():
      if type(self.dirs[key]) == dict:
        for dir_type in self.dirs[key].keys():
          create_if_not_exists(self.dirs[key][dir_type])
        numDirectories_created += len(self.dirs[key])
      else:
        create_if_not_exists(self.dirs[key])
        numDirectories_created = numDirectories_created + 1
      while 100*numDirectories_created >= frac*numDirectories:
        logging.info(" %i%% completed" % frac)
        frac = frac + 1
    logging.info("Done.")       

    inputFileLists = {}
    for sample_name, sample_info in self.samples.items():
      if not sample_info["use_it"]:
        continue
      logging.info("Checking input files for sample %s" % sample_info["process_name_specific"])
      inputFileLists[sample_name] = generateInputFileList(sample_info, self.max_files_per_job)
      
    mcClosure_regex = re.compile('Fakeable_mcClosure_(?P<type>m|e)_wFakeRateWeights')
    for lepton_selection in self.lepton_selections:
      electron_selection = lepton_selection
      muon_selection = lepton_selection

      hadTauVeto_selection = "Tight"
      hadTauVeto_selection = "|".join([ hadTauVeto_selection, self.hadTauVeto_selection_part2 ])

      if lepton_selection == "forBDTtraining":
        electron_selection = "Loose"
        muon_selection = "Loose"
      elif lepton_selection == "Fakeable_mcClosure_e":
        electron_selection = "Fakeable"
        muon_selection = "Tight"
      elif lepton_selection == "Fakeable_mcClosure_m":
        electron_selection = "Tight"
        muon_selection = "Fakeable"

      for lepton_frWeight in self.lepton_frWeights:
        if lepton_frWeight == "enabled" and not lepton_selection.startswith("Fakeable"):
          continue
        if lepton_frWeight == "disabled" and not lepton_selection in [ "Tight", "forBDTtraining" ]:
          continue
        lepton_selection_and_frWeight = get_lepton_selection_and_frWeight(lepton_selection, lepton_frWeight)

        for leptonChargeSelection in self.leptonChargeSelections:

          for sample_name, sample_info in self.samples.items():
            if not sample_info["use_it"]:
              continue
            process_name = sample_info["process_name_specific"]
            logging.info("Creating configuration files to run '%s' for sample %s" % (self.executable_analyze, process_name))
            inputFileList = inputFileLists[sample_name]

            sample_category = sample_info["sample_category"]
            is_mc = (sample_info["type"] == "mc")
            use_th_weights = self.runTHweights(sample_info)

            central_or_shift_dedicated = self.central_or_shifts if use_th_weights else self.central_or_shifts_external
            for central_or_shift in central_or_shift_dedicated:
              if not self.accept_systematics(
                  central_or_shift, is_mc, lepton_selection, leptonChargeSelection, sample_info
              ):
                continue

              central_or_shifts_local = []
              if central_or_shift == "central" and not use_th_weights:
                for central_or_shift_local in self.central_or_shifts_internal:
                  if self.accept_systematics(
                      central_or_shift_local, is_mc, lepton_selection, leptonChargeSelection, sample_info
                  ):
                    central_or_shifts_local.append(central_or_shift_local)

              logging.info(" ... for '%s' and systematic uncertainty option '%s'" % (lepton_selection_and_frWeight, central_or_shift))

              # build config files for executing analysis code
              key_analyze_dir = getKey(process_name, lepton_selection_and_frWeight, leptonChargeSelection, central_or_shift)

              for jobId in inputFileList.keys():
                analyze_job_tuple = (process_name, lepton_selection_and_frWeight, leptonChargeSelection, central_or_shift, jobId)
                key_analyze_job = getKey(*analyze_job_tuple)
                ntupleFiles = inputFileList[jobId]
                if len(ntupleFiles) == 0:
                  logging.warning("No input ntuples for %s --> skipping job !!" % (key_analyze_job))
                  continue

                syncOutput = ''
                syncTree = ''
                syncRequireGenMatching = False
                if self.do_sync:
                  if leptonChargeSelection != 'OS':
                    continue
                  mcClosure_match = mcClosure_regex.match(lepton_selection_and_frWeight)
                  if lepton_selection_and_frWeight == 'Tight':
                    syncOutput = os.path.join(self.dirs[key_dir][DKEY_SYNC], '%s_%s_SR.root' % (self.channel, central_or_shift))
                    syncTree   = 'syncTree_%s_SR' % self.channel.replace('_', '')
                    syncRequireGenMatching = True and is_mc
                  elif lepton_selection_and_frWeight == 'Fakeable_wFakeRateWeights':
                    syncOutput = os.path.join(self.dirs[key_dir][DKEY_SYNC], '%s_%s_Fake.root' % (self.channel, central_or_shift))
                    syncTree   = 'syncTree_%s_Fake' % self.channel.replace('_', '')
                  elif mcClosure_match:
                    mcClosure_type = mcClosure_match.group('type')
                    syncOutput = os.path.join(self.dirs[key_dir][DKEY_SYNC], '%s_%s_mcClosure_%s.root' % (self.channel, central_or_shift, mcClosure_type))
                    syncTree = 'syncTree_%s_mcClosure_%s' % (self.channel.replace('_', ''), mcClosure_type)
                  else:
                    continue
                if syncTree and central_or_shift != "central":
                  syncTree = os.path.join(central_or_shift, syncTree)
                syncRLE = ''
                if self.do_sync and self.rle_select:
                  syncRLE = self.rle_select % syncTree
                  if not os.path.isfile(syncRLE):
                    logging.warning("Input RLE file for the sync is missing: %s; skipping the job" % syncRLE)
                    continue
                if syncOutput:
                  self.inputFiles_sync['sync'].append(syncOutput)

                cfgFile_modified_path = os.path.join(self.dirs[key_analyze_dir][DKEY_CFGS], "analyze_%s_%s_%s_%s_%i_cfg.py" % analyze_job_tuple)
                logFile_path = os.path.join(self.dirs[key_analyze_dir][DKEY_LOGS], "analyze_%s_%s_%s_%s_%i.log" % analyze_job_tuple)
                rleOutputFile_path = os.path.join(self.dirs[key_analyze_dir][DKEY_RLES], "rle_%s_%s_%s_%s_%i.txt" % analyze_job_tuple) \
                                     if self.select_rle_output else ""
                histogramFile_path = os.path.join(self.dirs[key_analyze_dir][DKEY_HIST], "analyze_%s_%s_%s_%s_%i.root" % analyze_job_tuple)
                applyFakeRateWeights = self.applyFakeRateWeights \
                  if lepton_selection.find("Tight") == -1 \
                  else "disabled"

                self.jobOptions_analyze[key_analyze_job] = {
                  'ntupleFiles'              : ntupleFiles,
                  'cfgFile_modified'         : cfgFile_modified_path,
                  'histogramFile'            : histogramFile_path,
                  'logFile'                  : logFile_path,
                  'selEventsFileName_output' : rleOutputFile_path,
                  'electronSelection'        : electron_selection,
                  'muonSelection'            : muon_selection,
                  'apply_leptonGenMatching'  : self.apply_leptonGenMatching,
                  'hadTauSelection'          : hadTauVeto_selection,
                  'leptonChargeSelection'    : leptonChargeSelection,
                  'applyFakeRateWeights'     : applyFakeRateWeights,
                  'central_or_shift'         : central_or_shift,
                  'central_or_shifts_local'  : central_or_shifts_local,
                  'selectBDT'                : self.isBDTtraining,
                  'syncOutput'               : syncOutput,
                  'syncTree'                 : syncTree,
                  'syncRLE'                  : syncRLE,
                  'syncRequireGenMatching'   : syncRequireGenMatching,
                  'apply_hlt_filter'         : self.hlt_filter,
                  'useNonNominal'            : self.use_nonnominal,
                  'fillGenEvtHistograms'     : True,
                }
                self.createCfg_analyze(self.jobOptions_analyze[key_analyze_job], sample_info, lepton_selection)

                # initialize input and output file names for hadd_stage1
                key_hadd_stage1_dir = getKey(process_name, lepton_selection_and_frWeight, leptonChargeSelection)
                hadd_stage1_job_tuple = (process_name, lepton_selection_and_frWeight, leptonChargeSelection)
                key_hadd_stage1_job = getKey(*hadd_stage1_job_tuple)
                if not key_hadd_stage1_job in self.inputFiles_hadd_stage1:
                  self.inputFiles_hadd_stage1[key_hadd_stage1_job] = []
                self.inputFiles_hadd_stage1[key_hadd_stage1_job].append(self.jobOptions_analyze[key_analyze_job]['histogramFile'])
                self.outputFile_hadd_stage1[key_hadd_stage1_job] = os.path.join(self.dirs[key_hadd_stage1_dir][DKEY_HIST],
                                                                                "hadd_stage1_%s_%s_%s.root" % hadd_stage1_job_tuple)
                
            if self.isBDTtraining or self.do_sync:
              continue

            # add output files of hadd_stage1 to list of input files for hadd_stage1_5
            key_hadd_stage1_job = getKey(process_name, lepton_selection_and_frWeight, leptonChargeSelection)
            key_hadd_stage1_5_dir = getKey("hadd", lepton_selection_and_frWeight, leptonChargeSelection)
            hadd_stage1_5_job_tuple = (lepton_selection_and_frWeight, leptonChargeSelection)
            key_hadd_stage1_5_job = getKey(*hadd_stage1_5_job_tuple)
            if not key_hadd_stage1_5_job in self.inputFiles_hadd_stage1_5:
              self.inputFiles_hadd_stage1_5[key_hadd_stage1_5_job] = []
            self.inputFiles_hadd_stage1_5[key_hadd_stage1_5_job].append(self.outputFile_hadd_stage1[key_hadd_stage1_job])
            self.outputFile_hadd_stage1_5[key_hadd_stage1_5_job] = os.path.join(self.dirs[key_hadd_stage1_5_dir][DKEY_HIST],
                                                                        "hadd_stage1_5_%s_%s.root" % hadd_stage1_5_job_tuple)

          if self.isBDTtraining or self.do_sync:
            continue

          # sum fake background contributions for the total of all MC sample
          # input processes: TT_fake, TTW_fake, TTWW_fake, ...
          # output process: fakes_mc
          key_hadd_stage1_5_job = getKey(lepton_selection_and_frWeight, leptonChargeSelection)
          key_addBackgrounds_dir = getKey("addBackgrounds")
          addBackgrounds_job_fakes_tuple = ("fakes_mc", lepton_selection_and_frWeight, leptonChargeSelection)
          key_addBackgrounds_job_fakes = getKey(*addBackgrounds_job_fakes_tuple)
          sample_categories = []
          sample_categories.extend(self.nonfake_backgrounds)
          processes_input = []
          for sample_category in sample_categories:
            processes_input.append("%s_fake" % sample_category)
          self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_fakes] = {
            'inputFile' : self.outputFile_hadd_stage1_5[key_hadd_stage1_5_job],
            'cfgFile_modified' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_CFGS], "addBackgrounds_%s_%s_%s_cfg.py" % addBackgrounds_job_fakes_tuple),
            'outputFile' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_HIST], "addBackgrounds_%s_%s_%s.root" % addBackgrounds_job_fakes_tuple),
            'logFile' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_LOGS], "addBackgrounds_%s_%s_%s.log" % addBackgrounds_job_fakes_tuple),
            'categories' : [ getHistogramDir(category, lepton_selection, lepton_frWeight, leptonChargeSelection) for category in self.categories ],
            'processes_input' : processes_input,
            'process_output' : "fakes_mc"
          }
          self.createCfg_addBackgrounds(self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_fakes])

          # sum conversion background contributions for the total of all MC sample
          # input processes: TT_Convs, TTW_Convs, TTWW_Convs, ...
          # output process: Convs
          addBackgrounds_job_Convs_tuple = ("Convs", lepton_selection_and_frWeight, leptonChargeSelection)
          key_addBackgrounds_job_Convs = getKey(*addBackgrounds_job_Convs_tuple)
          sample_categories = []
          sample_categories.extend(self.nonfake_backgrounds)
          processes_input = []
          for sample_category in self.convs_backgrounds:
            processes_input.append("%s_Convs" % sample_category)
          self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_Convs] = {
            'inputFile' : self.outputFile_hadd_stage1_5[key_hadd_stage1_5_job],
            'cfgFile_modified' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_CFGS], "addBackgrounds_%s_%s_%s_cfg.py" % addBackgrounds_job_Convs_tuple),
            'outputFile' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_HIST], "addBackgrounds_%s_%s_%s.root" % addBackgrounds_job_Convs_tuple),
            'logFile' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_LOGS], "addBackgrounds_%s_%s_%s.log" % addBackgrounds_job_Convs_tuple),
            'categories' : [ getHistogramDir(category, lepton_selection, lepton_frWeight, leptonChargeSelection) for category in self.categories ],
            'processes_input' : processes_input,
            'process_output' : "Convs"
          }
          self.createCfg_addBackgrounds(self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_Convs])

          # sum signal contributions from HH->4tau ("tttt"), HH->2W2tau ("wwtt"), and HH->4W ("wwww"),
          # separately for "nonfake" and "fake" contributions
          genMatch_categories = [ "nonfake", "fake" ]
          for genMatch_category in genMatch_categories:
            for signal_base, signal_input in self.signal_io.items():
              addBackgrounds_job_signal_tuple = (lepton_selection_and_frWeight, leptonChargeSelection, signal_base, genMatch_category)
              key_addBackgrounds_job_signal = getKey(*addBackgrounds_job_signal_tuple)
              if key_addBackgrounds_job_signal in self.jobOptions_addBackgrounds_sum.keys():
                continue
              processes_input = signal_input
              process_output = signal_base
              if genMatch_category == "fake":
                processes_input = [ process_input + "_fake" for process_input in processes_input ]
                process_output += "_fake"
              self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_signal] = {
                'inputFile' : self.outputFile_hadd_stage1_5[key_hadd_stage1_5_job],
                'cfgFile_modified' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_CFGS], "addBackgrounds_%s_%s_%s_%s_cfg.py" % addBackgrounds_job_signal_tuple),
                'outputFile' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_HIST], "addBackgrounds_%s_%s_%s_%s.root" % addBackgrounds_job_signal_tuple),
                'logFile' : os.path.join(self.dirs[key_addBackgrounds_dir][DKEY_LOGS], "addBackgrounds_%s_%s_%s_%s.log" % addBackgrounds_job_signal_tuple),
                'categories' : [ getHistogramDir(category, lepton_selection, lepton_frWeight, leptonChargeSelection) for category in self.categories ],
                'processes_input' : processes_input,
                'process_output' : process_output
              }
              self.createCfg_addBackgrounds(self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_signal])
              key_hadd_stage2_job = getKey(lepton_selection_and_frWeight, leptonChargeSelection)
              if not key_hadd_stage2_job in self.inputFiles_hadd_stage2:
                self.inputFiles_hadd_stage2[key_hadd_stage2_job] = []
              if lepton_selection == "Tight":
                self.inputFiles_hadd_stage2[key_hadd_stage2_job].append(self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_signal]['outputFile'])

          # initialize input and output file names for hadd_stage2
          key_hadd_stage1_5_job = getKey(lepton_selection_and_frWeight, leptonChargeSelection)
          key_hadd_stage2_dir = getKey("hadd", lepton_selection_and_frWeight, leptonChargeSelection)
          hadd_stage2_job_tuple = (lepton_selection_and_frWeight, leptonChargeSelection)
          key_hadd_stage2_job = getKey(*hadd_stage2_job_tuple)
          if not key_hadd_stage2_job in self.inputFiles_hadd_stage2:
            self.inputFiles_hadd_stage2[key_hadd_stage2_job] = []
          if lepton_selection == "Tight":
            self.inputFiles_hadd_stage2[key_hadd_stage2_job].append(self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_fakes]['outputFile'])
            self.inputFiles_hadd_stage2[key_hadd_stage2_job].append(self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_Convs]['outputFile'])          
          self.inputFiles_hadd_stage2[key_hadd_stage2_job].append(self.outputFile_hadd_stage1_5[key_hadd_stage1_5_job])
          self.outputFile_hadd_stage2[key_hadd_stage2_job] = os.path.join(self.dirs[key_hadd_stage2_dir][DKEY_HIST],
                                                                          "hadd_stage2_%s_%s.root" % hadd_stage2_job_tuple)

    if self.isBDTtraining or self.do_sync:
      if self.is_sbatch:
        logging.info("Creating script for submitting '%s' jobs to batch system" % self.executable_analyze)
        self.sbatchFile_analyze = os.path.join(self.dirs[DKEY_SCRIPTS], "sbatch_analyze_%s.py" % self.channel)
        if self.isBDTtraining:
          self.createScript_sbatch_analyze(self.executable_analyze, self.sbatchFile_analyze, self.jobOptions_analyze)
        elif self.do_sync:
          self.createScript_sbatch_syncNtuple(self.executable_analyze, self.sbatchFile_analyze, self.jobOptions_analyze)
      logging.info("Creating Makefile")
      lines_makefile = []
      if self.isBDTtraining:
        self.addToMakefile_analyze(lines_makefile)
        self.addToMakefile_hadd_stage1(lines_makefile)
      elif self.do_sync:
        self.addToMakefile_syncNtuple(lines_makefile)
        outputFile_sync_path = os.path.join(self.outputDir, DKEY_SYNC, '%s.root' % self.channel)
        self.outputFile_sync['sync'] = outputFile_sync_path
        self.targets.append(outputFile_sync_path)
        self.addToMakefile_hadd_sync(lines_makefile)
      else:
        raise ValueError("Internal logic error")
      self.targets.extend(self.phoniesToAdd)
      self.addToMakefile_validate(lines_makefile)
      self.createMakefile(lines_makefile)
      logging.info("Done.")
      return self.num_jobs

    logging.info("Creating configuration files to run 'addBackgroundFakes'")
    for category in self.categories:
      for leptonChargeSelection in self.leptonChargeSelections:
        key_hadd_stage1_5_job = getKey(get_lepton_selection_and_frWeight("Fakeable", "enabled"), leptonChargeSelection)
        key_addFakes_dir = getKey("addBackgroundLeptonFakes")
        addFakes_job_tuple = (category, leptonChargeSelection)
        key_addFakes_job = getKey("data_fakes", *addFakes_job_tuple)
        self.jobOptions_addFakes[key_addFakes_job] = {
          'inputFile' : self.outputFile_hadd_stage1_5[key_hadd_stage1_5_job],
          'cfgFile_modified' : os.path.join(self.dirs[key_addFakes_dir][DKEY_CFGS], "addBackgroundLeptonFakes_%s_%s_cfg.py" % addFakes_job_tuple),
          'outputFile' : os.path.join(self.dirs[key_addFakes_dir][DKEY_HIST], "addBackgroundLeptonFakes_%s_%s.root" % addFakes_job_tuple),
          'logFile' : os.path.join(self.dirs[key_addFakes_dir][DKEY_LOGS], "addBackgroundLeptonFakes_%s_%s.log" % addFakes_job_tuple),
          'category_signal' : getHistogramDir(category, "Tight", "disabled", leptonChargeSelection),
          'category_sideband' : getHistogramDir(category, "Fakeable", "enabled", leptonChargeSelection)
        }
        self.createCfg_addFakes(self.jobOptions_addFakes[key_addFakes_job])
        key_hadd_stage2_job = getKey(get_lepton_selection_and_frWeight("Tight", "disabled"), leptonChargeSelection)
        self.inputFiles_hadd_stage2[key_hadd_stage2_job].append(self.jobOptions_addFakes[key_addFakes_job]['outputFile'])

    logging.info("Creating configuration files to run 'prepareDatacards'")
    for category in self.categories:
      for histogramToFit in self.histograms_to_fit:
        key_prep_dcard_dir = getKey("prepareDatacards")
        if "OS" in self.leptonChargeSelections:
          key_hadd_stage2_job = getKey(get_lepton_selection_and_frWeight("Tight", "disabled"), "OS")
          prep_dcard_job_tuple = (self.channel, category, "OS", histogramToFit)
          key_prep_dcard_job = getKey(category, "OS", histogramToFit)
          self.jobOptions_prep_dcard[key_prep_dcard_job] = {
            'inputFile' : self.outputFile_hadd_stage2[key_hadd_stage2_job],
            'cfgFile_modified' : os.path.join(self.dirs[key_prep_dcard_dir][DKEY_CFGS], "prepareDatacards_%s_%s_%s_%s_cfg.py" % prep_dcard_job_tuple),
            'datacardFile' : os.path.join(self.dirs[key_prep_dcard_dir][DKEY_DCRD], "prepareDatacards_%s_%s_%s_%s.root" % prep_dcard_job_tuple),
            'histogramDir' : getHistogramDir(category, "Tight", "disabled", "OS"),
            'histogramToFit' : histogramToFit,
            'label' : '3l'
          }
          self.createCfg_prep_dcard(self.jobOptions_prep_dcard[key_prep_dcard_job])

        if "SS" in self.leptonChargeSelections:
          key_hadd_stage2_job = getKey(get_lepton_selection_and_frWeight("Tight", "disabled"), "SS")
          prep_dcard_job_tuple = (self.channel, category, "SS", histogramToFit)
          key_prep_dcard_job = getKey(category, "SS", histogramToFit)
          self.jobOptions_prep_dcard[key_prep_dcard_job] = {
            'inputFile' : self.outputFile_hadd_stage2[key_hadd_stage2_job],
            'cfgFile_modified' : os.path.join(self.dirs[key_prep_dcard_dir][DKEY_CFGS], "prepareDatacards_%s_%s_%s_%s_cfg.py" % prep_dcard_job_tuple),
            'datacardFile' : os.path.join(self.dirs[key_prep_dcard_dir][DKEY_DCRD], "prepareDatacards_%s_%s_%s_%s.root" % prep_dcard_job_tuple),
            'histogramDir' : getHistogramDir(category, "Tight", "disabled", "SS"),
            'histogramToFit' : histogramToFit,
            'label' : '3l SS'
          }
          self.createCfg_prep_dcard(self.jobOptions_prep_dcard[key_prep_dcard_job])

        # add shape templates for the following systematic uncertainties:
        #  - 'CMS_ttHl_Clos_norm_e'
        #  - 'CMS_ttHl_Clos_shape_e'
        #  - 'CMS_ttHl_Clos_norm_m'
        #  - 'CMS_ttHl_Clos_shape_m'
        for leptonChargeSelection in self.leptonChargeSelections:          
          key_prep_dcard_job = getKey(category, leptonChargeSelection, histogramToFit)
          key_hadd_stage2_job = getKey(get_lepton_selection_and_frWeight("Tight", "disabled"), leptonChargeSelection)
          key_add_syst_fakerate_dir = getKey("addSystFakeRates")
          add_syst_fakerate_job_tuple = (self.channel, category, leptonChargeSelection, histogramToFit)
          key_add_syst_fakerate_job = getKey(category, leptonChargeSelection, histogramToFit)
          self.jobOptions_add_syst_fakerate[key_add_syst_fakerate_job] = {
            'inputFile' : self.jobOptions_prep_dcard[key_prep_dcard_job]['datacardFile'],
            'cfgFile_modified' : os.path.join(self.dirs[key_add_syst_fakerate_dir][DKEY_CFGS], "addSystFakeRates_%s_%s_%s_%s_cfg.py" % add_syst_fakerate_job_tuple),
            'outputFile' : os.path.join(self.dirs[key_add_syst_fakerate_dir][DKEY_DCRD], "addSystFakeRates_%s_%s_%s_%s.root" % add_syst_fakerate_job_tuple),
            'category' : category,
            'histogramToFit' : histogramToFit,
            'plots_outputFileName' : os.path.join(self.dirs[DKEY_PLOT], "addSystFakeRates.png")
          }
          histogramDir_nominal = getHistogramDir(category, "Tight", "disabled", leptonChargeSelection)  
          for lepton_type in [ 'e', 'm' ]:
            lepton_mcClosure = "Fakeable_mcClosure_%s" % lepton_type
            if lepton_mcClosure not in self.lepton_selections:
              continue
            lepton_selection_and_frWeight = get_lepton_selection_and_frWeight(lepton_mcClosure, "enabled")
            key_addBackgrounds_job_fakes = getKey("fakes_mc", lepton_selection_and_frWeight, leptonChargeSelection)
            histogramDir_mcClosure = self.mcClosure_dir['%s_%s' % (lepton_mcClosure, leptonChargeSelection)]
            self.jobOptions_add_syst_fakerate[key_add_syst_fakerate_job].update({
              'add_Clos_%s' % lepton_type : ("Fakeable_mcClosure_%s" % lepton_type) in self.lepton_selections,
              'inputFile_nominal_%s' % lepton_type : self.outputFile_hadd_stage2[key_hadd_stage2_job],
              'histogramName_nominal_%s' % lepton_type : "%s/sel/evt/fakes_mc/%s" % (histogramDir_nominal, histogramToFit),
              'inputFile_mcClosure_%s' % lepton_type : self.jobOptions_addBackgrounds_sum[key_addBackgrounds_job_fakes]['outputFile'],
              'histogramName_mcClosure_%s' % lepton_type : "%s/sel/evt/fakes_mc/%s" % (histogramDir_mcClosure, histogramToFit)
            })
          self.createCfg_add_syst_fakerate(self.jobOptions_add_syst_fakerate[key_add_syst_fakerate_job])

    logging.info("Creating configuration files to run 'makePlots'")
    key_makePlots_dir = getKey("makePlots")
    if "OS" in self.leptonChargeSelections:
      key_hadd_stage2_job = getKey(get_lepton_selection_and_frWeight("Tight", "disabled"), "OS")
      key_makePlots_job = getKey("OS")
      self.jobOptions_make_plots[key_makePlots_job] = {
        'executable' : self.executable_make_plots,
        'inputFile' : self.outputFile_hadd_stage2[key_hadd_stage2_job],
        'cfgFile_modified' : os.path.join(self.dirs[key_makePlots_dir][DKEY_CFGS], "makePlots_%s_cfg.py" % self.channel),
        'outputFile' : os.path.join(self.dirs[key_makePlots_dir][DKEY_PLOT], "makePlots_%s.png" % self.channel),
        'histogramDir' : getHistogramDir(self.category_inclusive, "Tight", "disabled", "OS"),
        'label' : "WZctrl_SFstudy",
        'make_plots_backgrounds' : self.make_plots_backgrounds,
      }
      self.createCfg_makePlots(self.jobOptions_make_plots[key_makePlots_job])

    if "SS" in self.leptonChargeSelections:
      key_hadd_stage2_job = getKey(get_lepton_selection_and_frWeight("Tight", "disabled"), "SS")
      key_makePlots_job = getKey("SS")
      self.jobOptions_make_plots[key_makePlots_job] = {
        'executable' : self.executable_make_plots,
        'inputFile' : self.outputFile_hadd_stage2[key_hadd_stage2_job],
        'cfgFile_modified' : os.path.join(self.dirs[key_makePlots_dir][DKEY_CFGS], "makePlots_%s_SS_cfg.py" % self.channel),
        'outputFile' : os.path.join(self.dirs[key_makePlots_dir][DKEY_PLOT], "makePlots_%s_SS.png" % self.channel),
        'histogramDir' : getHistogramDir(self.category_inclusive, "Tight", "disabled", "SS"),
        'label' : "HH 3l SS",
        'make_plots_backgrounds' : self.make_plots_backgrounds,
      }
      self.createCfg_makePlots(self.jobOptions_make_plots[key_makePlots_job])
    if "Fakeable_mcClosure" in self.lepton_selections: #TODO
      key_hadd_stage2_job = getKey(get_lepton_selection_and_frWeight("Tight", "disabled"), "OS")
      key_makePlots_job = getKey("Fakeable_mcClosure", "OS")      
      self.jobOptions_make_plots[key_makePlots_job] = {
        'executable' : self.executable_make_plots_mcClosure,
        'inputFile' : self.outputFile_hadd_stage2[key_hadd_stage2_job],
        'cfgFile_modified' : os.path.join(self.dirs[key_makePlots_dir][DKEY_CFGS], "makePlots_mcClosure_%s_cfg.py" % self.channel),
        'outputFile' : os.path.join(self.dirs[key_makePlots_dir][DKEY_PLOT], "makePlots_mcClosure_%s.png" % self.channel)
      }
      self.createCfg_makePlots_mcClosure(self.jobOptions_make_plots[key_makePlots_job])

    if self.is_sbatch:
      logging.info("Creating script for submitting '%s' jobs to batch system" % self.executable_analyze)
      self.sbatchFile_analyze = os.path.join(self.dirs[DKEY_SCRIPTS], "sbatch_analyze_%s.py" % self.channel)
      self.createScript_sbatch_analyze(self.executable_analyze, self.sbatchFile_analyze, self.jobOptions_analyze)
      logging.info("Creating script for submitting '%s' jobs to batch system" % self.executable_addBackgrounds)
      self.sbatchFile_addBackgrounds = os.path.join(self.dirs[DKEY_SCRIPTS], "sbatch_addBackgrounds_%s.py" % self.channel)
      self.createScript_sbatch(self.executable_addBackgrounds, self.sbatchFile_addBackgrounds, self.jobOptions_addBackgrounds)
      self.sbatchFile_addBackgrounds_sum = os.path.join(self.dirs[DKEY_SCRIPTS], "sbatch_addBackgrounds_sum_%s.py" % self.channel)
      self.createScript_sbatch(self.executable_addBackgrounds, self.sbatchFile_addBackgrounds_sum, self.jobOptions_addBackgrounds_sum)
      logging.info("Creating script for submitting '%s' jobs to batch system" % self.executable_addFakes)
      self.sbatchFile_addFakes = os.path.join(self.dirs[DKEY_SCRIPTS], "sbatch_addFakes_%s.py" % self.channel)
      self.createScript_sbatch(self.executable_addFakes, self.sbatchFile_addFakes, self.jobOptions_addFakes)

    logging.info("Creating Makefile")
    lines_makefile = []
    self.addToMakefile_analyze(lines_makefile)
    self.addToMakefile_hadd_stage1(lines_makefile)
    self.addToMakefile_backgrounds_from_data(lines_makefile)
    #self.addToMakefile_hadd_stage2(lines_makefile)
    #----------------------------------------------------------------------------
    # CV: run hadd_stage2 jobs on quasar,
    #     as the memory consumption of hadd_stage2 jobs exceeds the memory limit (1.8 Gb) for batch jobs
    is_sbatch_bak = self.is_sbatch
    self.is_sbatch = False
    is_makefile_bak = self.is_makefile
    self.is_makefile = True
    self.addToMakefile_hadd_stage2(lines_makefile)
    self.is_sbatch = is_sbatch_bak
    self.is_makefile = is_makefile_bak
    #----------------------------------------------------------------------------
    self.addToMakefile_prep_dcard(lines_makefile)
    self.addToMakefile_add_syst_fakerate(lines_makefile)
    self.addToMakefile_make_plots(lines_makefile)
    self.addToMakefile_validate(lines_makefile)
    self.createMakefile(lines_makefile)

    logging.info("Done.")

    return self.num_jobs