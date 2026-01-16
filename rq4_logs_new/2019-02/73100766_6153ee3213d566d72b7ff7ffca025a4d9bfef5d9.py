#!/usr/bin/env python

from __future__ import division, unicode_literals, print_function, \
    absolute_import

"""
This script serves as a management tool for vasp projects, starting
from encut, kpoint or other parameter optimization of till the slab
solvation. Just define all types of calculations with their
corresponding specifications needed for the project in a yaml file
and run or rerun calculaitons as required.

Note: use your own materials project key to download the required
structure
"""

from six.moves import range

import os
import shutil
import yaml
from argparse import ArgumentParser
from fnmatch import fnmatch
from glob import glob
import ast
import subprocess


from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymatgen.io.vasp.inputs import Incar
from pymatgen.io.vasp.inputs import Potcar, Kpoints
from pymatgen.io.vasp.outputs import Oszicar

from mpinterfaces import *
from mpinterfaces.utils import *
from mpinterfaces.mpint_parser import *
from mpinterfaces.workflows import *
from mpinterfaces.calibrate import Calibrate
from mpinterfaces.interface import Interface
from mpinterfaces.default_logger import get_default_logger


MAPI_KEY = os.environ.get("MAPI_KEY", "")
vasp_config = {'twod_binary': VASP_TWOD_BIN,
               'bulk_binary': VASP_STD_BIN,
               'ncl_binary': VASP_NCL_BIN,
               'sol_binary': VASP_SOL_BIN,
               'custom_binary': VASP_CUSTOM_BIN}

def process_dir(val):
    poscar_list = []
    if os.path.isdir(val):
        for f in os.listdir(val):
            fname = os.path.join(val, f)
            if os.path.isfile(fname) and fnmatch(fname, "*POSCAR*"):
                poscar_list.append(Poscar.from_file(fname))
    return poscar_list


def process_input(args):
        if args.command == 'start_project':
           if args.i:
             f = open(args.i)
             my_project = yaml.load(open(args.i)) ## this will be the only CLI input
             f.close()
             NAME = my_project['NAME']

             INCAR_GENERAL = my_project['Incar_General']
             POTCAR_SPEC = yaml.load(open(my_project['Potcar_Spec']))

             MATERIALS_LIST = my_project['Insilico_Fab']['Material_List']
             struct_list = [Poscar.from_file(poscar) for poscar in glob('StructsDir/*.vasp') \
                   if 'StructsDir' in MATERIALS_LIST] + \
                  [Poscar(get_struct_from_mp(p)) for p in MATERIALS_LIST \
                   if 'StructsDir' not in p]
             WORKFLOWS = my_project['Workflow']
             project_log=get_logger(NAME+"_InSilico_Materials")

             error_handler = [VaspErrorHandler()]
             Order_WfNames = list(np.sort(list(WORKFLOWS['Steps'].keys())))
             steps_map = {'StepVASP0':StepVASP0,'StepVASP1':StepVASP1}
             steps_dict = {k:WORKFLOWS['Steps'][k]['TYPE'] for k in Order_WfNames}
             steps_map[steps_dict[list(steps_dict.keys())[0]]](my_project,struct_list)

             project_abs_dir = os.path.abspath(os.path.curdir)
             my_project['Project_Dir'] = project_abs_dir
             my_project['Running_Wflow'] = [int(Order_WfNames[0])]

             with open(args.i, 'w') as projfile:
                yaml.dump(my_project, projfile, default_flow_style=False)
             if os.path.exists('custodian.json'):
                os.remove('custodian.json')
             projfile.close()

        if args.command == 'continue_project':  
           if args.i:
             f = open(args.i)
             my_project = yaml.load(open(args.i)) ## this will be the only CLI input
             f.close()
             NAME = my_project['NAME']

             WORKFLOWS = my_project['Workflow']

             #error_handler = [VaspErrorHandler()]
             Order_WfNames = list(np.sort(list(WORKFLOWS['Steps'].keys())))
             steps_map = {'StepVASP0':StepVASP0,'StepVASP1':StepVASP1}
             steps_dict = {k:WORKFLOWS['Steps'][k]['TYPE'] for k in Order_WfNames}
             for k in Order_WfNames:
                if k not in my_project['Running_Wflow']:
                  #print (k)
                  #print (steps_map[steps_dict[list(steps_dict.keys())[k]]])
                  steps_map[steps_dict[list(steps_dict.keys())[k]]](my_project,k)
                  #print ('Here')
                  orig_done = my_project['Running_Wflow']
                  orig_done.append(k)
                  my_project['Running_Wflow'] = [int(o) for o in orig_done]
                  with open(args.i, 'w') as projfile:
                    yaml.dump(my_project, projfile, default_flow_style=False)
                  if os.path.exists('custodian.json'):
                    os.remove('custodian.json')
                  projfile.close()
                  break

        if args.command == 'check_project':
           # check if any input spec for the project 
           if args.i:
              f = open(args.i)
              project_spec = yaml.load(f)
              if args.c:
                  workflow_chkpts = [args.c]
              else:
                  workflow_chkpts = glob('{}*.json'.format(project_spec['NAME']))
              #print (workflow_chkpts)
              proj_dir = project_spec['Project_Dir']
              os.chdir(proj_dir)
              CustodianChecks=\
                {chk:check_errors(chkfile=chk,logfile_name=\
                     'Custodian_'+project_spec['NAME']) for chk in workflow_chkpts}
              with open('{}_CustodianReport.yaml'.format(project_spec['NAME']), 'w') as report:
                 yaml.dump(CustodianChecks, report, default_flow_style=False)
              report.close()      
              
        elif args.command == 'rerun_project':
           # Custodian yamls are input
           print (args.i,len(args.i))
           if args.i:
              f = open(args.i)
              rerun_logs = get_logger('{}_reruns'.format(args.i.replace('.yaml','')))
              rerun_spec = yaml.load(f)
              proj_dir = os.path.abspath(os.path.curdir)
              if args.c:
                  rerun_chkpts = [args.c]
              else:
                  rerun_chkpts = list(rerun_spec.keys())
              print (rerun_chkpts)
              for k in rerun_chkpts:
                  for case in rerun_spec[k]:
                     print ('Rerunning {}'.format(case['ErrorDir'][0]))
                     rerun_logs.info('Rerunning {}'.format(case['ErrorDir'][0]))
                     if args.s:
                        rerun_logs.info('Using new submit_file {} for all rerun'.format(args.s))
                        os.system('cp {0} {1}'.format(args.s,case['ErrorDir'][0]))
                        submit_cmd = ['sbatch',args.s]
                     else:
                        if case['Error']==['Memory Error']:
                          if args.m:
                             rerun_logs.info('Error Memory adding {}'.format(args.m))
                             add_mem_submit_file(case['ErrorDir'][0]+'/submit_script',args.m)
                          else:
                             rerun_logs.info('Error Memory adding 3000')
                             add_mem_submit_file(case['ErrorDir'][0]+'/submit_script',3000)
                        elif 'TIME LIMIT' in case['Error'][0]:
                          if args.w:
                             rerun_logs.info('Error TIME LIMIT adding {}'.format(args.w))
                             add_walltime(case['ErrorDir'][0]+'/submit_script',args.w)
                          else:
                             rerun_logs.info('Error TIME LIMIT adding 20:00:00')
                             add_walltime(case['ErrorDir'][0]+'/submit_script','20:00:00')
                        submit_cmd = ['sbatch','submit_script']

                     os.chdir(case['ErrorDir'][0])

                     if args.inc:
                        incar = Incar.from_file('INCAR')
                        user_dict = ast.literal_eval(args.inc)
                        incar.update(user_dict)
                        incar.write_file('INCAR')
                     if args.dinc:
                        incar = Incar.from_file('INCAR')
                        user_exp = ast.literal_eval(args.dinc)
                        for d in user_exp:
                            if d in list(incar.keys()):
                              del incar[d]
                        incar.write_file('INCAR')
                     if args.kpt:
                        user_exp = ast.literal_eval(args.kpt)
                        if isinstance(user_exp,tuple):
                           kpoints = Kpoints.gamma_automatic(user_exp)
                        else:
                           struct = Structure.from_file('POSCAR')
                           kpoints = Kpoints.automatic_gamma_density(struct,user_exp)
                        kpoints.write_file('KPOINTS')
                        
                     p = subprocess.Popen(['sbatch', 'submit_script'], stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                     stdout, stderr = p.communicate()
                     rerun_job_id = str(stdout).rstrip('\n').split()[-1].replace("\\n'",'')
                     rerun_logs.info('running job {0} in {1}'.format(rerun_job_id, case['ErrorDir'][0]))
                     os.chdir(proj_dir)
                      
              rerun_logs.info('Finished submitting reruns')
              print ('Finished submitting reruns')

        elif args.command == 'analyze_project':
           # check for yaml analysis input for project
           if args.i:
              f = open(args.i)
              proj_spec = yaml.load(f)
              proj_wflow_st = proj_spec['Workflow']['Steps'] 
              for step in proj_wflow_st:
                print (step)
                if 'Analysis' in list(proj_wflow_st[step].keys()):
                   analyze_script = proj_wflow_st[step]['Analysis']['Script']
                   analyze_input = proj_wflow_st[step]['Analysis']['Input']
                   analyze_output = proj_wflow_st[step]['Analysis']['Output']
                   if '.py' in analyze_script:
#                       os.system('python {0} -i {1} -o {2}'.format(analyze_script, analyze_input, analyze_output))
                       print (analyze_script, analyze_input, analyze_output)
                       p = subprocess.Popen(['python',analyze_script, '-i', analyze_input,\
                           '-o', analyze_output], stdout=subprocess.PIPE, \
                           stderr=subprocess.PIPE)
                       stdout, stderr = p.communicate()
                   print (stdout)
              print ('Analyzed the project according to specified post processing script')
              

        elif args.command == 'archive_project':
           # check for workflow.yaml input file 
           if args.i:
              print ('tar.gz the project json files and csv and vasprun.xml files from the project directory')
              f = open(args.i)
              proj_spec = yaml.load(f)
              name_spec = proj_spec['NAME']
              proj_dir = proj_spec['Project_Dir']
              os.chdir(proj_dir)
              # first the json checkpoints
              os.system('tar cvzf {}.tar.gz {}*.json '.format(name_spec,name_spec))
              # then add all vaspruns to tar archive
              os.system('find . -iname "*.xml" -exec tar -rvf {0}.tar {} \;'.format(name_spec+'_vaspruns_csvs'))
              # then add csvs
              os.system('find . -iname "*.csv" -exec tar -rvf {0}.tar {} \;'.format(name_spec+'_vaspruns_csvs'))
              # compress the archive
              os.system('tar cvzf {}.tar.gz {}.tar'.format(name_spec+'_vaspruns_csvs'))
              # finally delete WAVECARS and CHG, CHGCARS
              os.system('find . -iname "WAVECAR" -exec rm {} \;')
              os.system('find . -iname "CHG*" -exec rm {} \;')

        elif args.command == 'load_settings':
           if args.i:
              user_dict = ast.literal_eval(args.i)
              if not os.path.exists(SETTINGS_FILE):
                 user_configs = {key:None for key in ['username','bulk_binary','twod_binary',\
                      'ncl_binary', 'sol_binary', 'custom_binary',\
                      'vdw_kernel','potentials','MAPI_KEY', 'queue_system', 'queue_template']}
                 with open(os.path.join(os.path.expanduser('~'),'.mpint_config.yaml'),'w') \
                   as config_file:
                         yaml.dump(user_configs, config_file, default_flow_style=False)
              config_data = yaml.load(open(SETTINGS_FILE))
              config_data.update(user_dict)
              load_config_vars(config_data)

        elif args.command == 'qcheck_project':
           states = []
           if args.i:
              f = open(args.i)
              project_spec = yaml.load(f)
              workflow_logs = [fi for fi in glob('{}*.log'.format(project_spec['NAME'])) if 'InSilico' not in fi]
              for l in workflow_logs:
                  states = []
                  print ('Qcheck on {}'.format(l))
                  tot, job_id, job_dir, job_name = decode_log_file(l)
                  for n, j in enumerate(job_id):
                       state,oszi,nsw= decode_q(j,job_dir[n])
                       print (state,job_dir[n],j)
                       if state == 'R' and isinstance(oszi,Oszicar):
                          try:
                              print ('Ionic Steps', len(oszi.ionic_steps),nsw,oszi.ionic_steps[-1])
                          except: 
                              print ('First Ionic Step', oszi.electronic_steps)
                       states.append(state)
                  running_states = [s for s in states if s=='R']
                  print ('{0} of {1} total jobs running'.format(len(running_states),len(job_id)))


        elif args.command == 'cancel_project':
           states = []
           if args.i:
              f = open(args.i)
              project_spec = yaml.load(f)
              workflow_logs = [fi for fi in glob('{}*.log'.format(project_spec['NAME'])) if 'InSilico' not in fi]
              for l in workflow_logs:
                  states = []
                  print ('Qcheck on {}'.format(l))
                  tot, job_id, job_dir, job_name = decode_log_file(l)
                  for n, j in enumerate(job_id):
                      os.system('scancel {}'.format(str(job_id)))


def main():
   args = mpint_parse_arguments(sys.argv[1:])
   if args.command:
      process_input(args)
            
if __name__ == '__main__':
    main()