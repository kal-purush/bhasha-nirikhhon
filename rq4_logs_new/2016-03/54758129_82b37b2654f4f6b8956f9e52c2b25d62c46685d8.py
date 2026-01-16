import os, math
import time
import datetime
from libtbx import easy_run
import iotbx.pdb
import re
import sys
import restraints
from mmtbx import model_statistics
from scitbx.array_family import flex

def get_model_stat(file_name):
  grm = restraints.get_grm(file_name = file_name)
  r = model_statistics.geometry(
    pdb_hierarchy      = grm.pdb_hierarchy,
    restraints_manager = grm.restraints_manager,
    molprobity_scores  = True)
  # XXX very runtime inefficient
  distances = flex.double()
  xyz = grm.pdb_hierarchy.atoms().extract_xyz()
  bond_proxies_simple = grm.restraints_manager.geometry.pair_proxies(
    sites_cart = xyz).bond_proxies.simple
  for i, site_i in enumerate(xyz):
    for j, site_j in enumerate(xyz):
      if(j>i):
        bonded = False
        for proxy in bond_proxies_simple:
          p1 = list(proxy.i_seqs)
          p2 = [i,j]
          p1.sort()
          p2.sort()
          if(p1==p2): bonded=True
        if(not bonded):
          dist_ij = math.sqrt(
            (site_i[0]-site_j[0])**2+
            (site_i[1]-site_j[1])**2+
            (site_i[2]-site_j[2])**2)
          distances.append(dist_ij)
  min_nonbonded_distance = flex.min(distances)
  # bond(rmsd), bond(max), angle(rmsd), angle(max), etc..
  #print r.b_mean, r.b_max, r.a_min, r.a_max, r.clashscore, min_nonbonded_distance
  return min_nonbonded_distance, r.b_max , r.a_max

def run():
  total_start_time = time.time()
  # These are the 4 strategies
  #               Quantum REFINE  Classical Refine   Quantum Optimization             Classical Optimization
  strategies = {"refine_cctbx":"restraints=cctbx ","refine_qm":"restraints=qm ","opt_qm":"restraints=qm data_weight=0","opt_cctbx":"restraints=cctbx data_weight=0 "}
  print "Strategies selected are: "

  # This gives us the ability to direct the output PDBs of the script
  results_prefix = "../p26/04_results/"
  perturbed_prefix="../p26/03_perturbed/"
  mtz_prefix="../p26/02_mtz/"
  p26_pdb_prefix="../p26/01d_fixSplit_GFA/"
  #
  folder_name=results_prefix
  make_folder(folder_name)

  #Loop over refinement or optimization strategies
  strategy_start_time = 0
  print "structure_name","success_number","mc_cycles","cal_time","cal_steps",\
             "rmsd_to_p26","restraint_scale_end","R_start","R_end","rmsd(b)_start","rmsd(b)_end"
  for strategy_name,strategy in strategies.iteritems():
    if(strategy_name=="refine_qm" or strategy_name=="refine_cctbx" ):  
      print "["+strategy_name+"]"
      strategy_start_time = time.time()
      folder_name=results_prefix+strategy_name
      make_folder(folder_name)
      file_out_prefix = [results_prefix]
      file_out_prefix.append(strategy_name)
      #Loop over data files
      data_files = os.listdir(mtz_prefix)
#      print "p26_file_name ",  " minimum_nobond", " maximum_rms_deltas(b)", "maximum_rms_deltas(a)"
#      for data_file in data_files:
#        data_file_start_time = time.time()
#        if(data_file.endswith(".mtz")):
#          data_file_name = data_file[:-4] # drop the .mtz extension
#          p26_pdb_file = p26_pdb_prefix+data_file_name+".pdb"
#          stat_object = get_model_stat(file_name = p26_pdb_file)
#          print p26_pdb_file, stat_object 
      for data_file in data_files:
        data_file_start_time = time.time()
        if(data_file.endswith(".mtz")):
          data_file_name = data_file[:-4] # drop the .mtz extension
          p26_pdb_file = p26_pdb_prefix+data_file_name+".pdb"      
          p26_pdb_xray = iotbx.pdb.input(
            file_name = p26_pdb_file).xray_structure_simple()
          folder_name=results_prefix+strategy_name+"/"+data_file_name
          make_folder(folder_name)
          file_out_prefix.append(data_file_name)
          pertubations = os.listdir(perturbed_prefix+data_file_name+"/")
          for pertubation in pertubations:
            if(os.path.isdir(perturbed_prefix+"/"+data_file_name+"/"+pertubation)):
              file_out_prefix.append(pertubation)
              make_folder(results_prefix+strategy_name+"/"+data_file_name+"/"+pertubation)
          #Loop over Snapshots
              snapshotdir_name=perturbed_prefix+data_file_name+"/"+pertubation+"/"
              snapshots = os.listdir(snapshotdir_name)
              mc_cycles, cal_time, cal_steps,rmsd_to_p26,restraint_scale_end,R_start,\
		R_end,rmsd_b_start,rmsd_b_end=0,0,0,0,0,0,0,0,0
              job_failed=False
	      geometry_error=False
	      convergence_failed=True
	      counter=10.0
              for snapshot in snapshots:
                if(snapshot.endswith("pdb")):
                  snapshot_file_name = snapshot[:-4]
                  file_out_prefix.append(snapshot_file_name)
                  output=results_prefix+strategy_name+"/"+data_file_name+"/"+\
                    pertubation+"/"+snapshot_file_name
                  result_pdb= output+".pdb"
                  result_log=output+".log"
                  structure_name=data_file_name+"/"+pertubation+"/"+snapshot_file_name
                  result_pdb_exists=os.path.exists(result_pdb)
                  if(result_pdb_exists != True):
		    job_failed=True
                    print structure_name, " The job  failed"
		    counter=counter-1.0
	            continue		
                  else:
                    #minimum_nobond=0#=??
                    #maximum_rms_deltas_b=0#=??
                    minimum_nobond, maximum_rms_deltas_b, maximum_rms_deltas_a=get_model_stat(result_pdb)
                    if(minimum_nobond <1.3 or  maximum_rms_deltas_b > 0.3 or maximum_rms_deltas_a>30):
                      geometry_error=True
                      print structure_name, "geometry error"
	              counter=counter-1.0
                      continue   
                    log_file = open(result_log,"r").readlines()
		    cal_time=float((log_file[-1].split())[1])+cal_time	
                    data_start=0
                    data_end=0
                    for line in log_file:
                      if 'End of input processing' in line:
                        data_start=log_file.index(line)+1
                      if "see the result structure" in line:
                        data_end=log_file.index(line)-1
		      if "Convergence reached" in line:
			convergence_failed=False 
                    if(convergence_failed==True):
		      print structure_name,"not converged"
		      counter=counter-1.0
                      continue   		      
                    cols=[]
                    for line in log_file[data_start:data_end]:
                      line= line.replace("="," ")
                      col=line.split()
                      cols.append(col)
                    R_start=float(cols[0][3])+R_start
                    R_end=float(cols[-1][3])+R_end
                    rmsd_b_start=float(cols[3][2])+rmsd_b_start
                    rmsd_b_end=float(cols[-1][5])+rmsd_b_end
                    restraint_scale_end=float(cols[-1][7])+restraint_scale_end
                    mc_cycles=int(cols[-1][1])+1+mc_cycles
                    minimization_steps=[]
                    for col in cols[4:]:
                      minimization_steps.append(int(col[-1]))
                    cal_steps=sum(minimization_steps)+cal_steps
                    result_pdb_xray = iotbx.pdb.input(
                      file_name = result_pdb).xray_structure_simple()
                    rmsd_to_p26 = p26_pdb_xray.sites_cart().rms_difference(
                      result_pdb_xray.sites_cart())+rmsd_to_p26
                   # print (structure_name,geometry_error,minimum_nobond,
                    #       maximum_rms_deltas_b,maximum_rms_deltas_a,mc_cycles,cal_time,cal_steps,
                     #      rmsd_to_p26,restraint_scale_end,R_start,R_end,
                      #     rmsd_b_start,rmsd_b_end)
              #if (job_failed==False and geometry_error==False and convergence_failed==False):
              if (counter!=0):
                print data_file_name+"/"+pertubation, counter ,mc_cycles/counter,cal_time/counter,cal_steps/counter,rmsd_to_p26/counter,\
                     restraint_scale_end/counter,R_start/counter,R_end/counter,rmsd_b_start/counter,rmsd_b_end/counter
      strategy_time = time.time() - strategy_start_time
      print "Time taken for " ,strategy ,"was ", strategy_time
  total_time = time.time() - total_start_time
  print "Time taken for entire batch was:", total_time

def  make_folder(folder_name):
   if(os.path.exists(folder_name)!=True):
       os.mkdir(folder_name)

if (__name__ == "__main__"):
  run()