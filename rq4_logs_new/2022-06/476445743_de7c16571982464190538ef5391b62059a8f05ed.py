import os
import numpy as np
import argparse
import subprocess
import json


def argparser():
    '''Define arguments'''

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Run AMELLIE simulation and subsequent analysis code for list of sim info')

    parser.add_argument('--macro_repo', '-mar', type=str, dest='macro_repo',
                        default='/mnt/lustre/scratch/epp/jp643/AMELLIE/macros/', help='Folder to save Region-selected root files in.')
    parser.add_argument('--sim_repo', '-sir', type=str, dest='sim_repo',
                        default='/mnt/lustre/scratch/epp/jp643/AMELLIE/sims/', help='Folder to save intial root files from AMELLIE simulations in.')
    parser.add_argument('--hist_repo', '-hir', type=str, dest='hist_repo',
                        default='/mnt/lustre/scratch/epp/jp643/AMELLIE/hists/', help='Folder to save root files with tracking information in.')
    parser.add_argument('--stats_repo', '-str', type=str, dest='stats_repo',
                        default='/mnt/lustre/scratch/epp/jp643/AMELLIE/stats/', help='Folder to save stats txt files in.')

    parser.add_argument('--nevts', '-n', type=int, dest='nevts',
                        default=10, help='Number of events to simulate for each setting')
    parser.add_argument('--list', '-l', type=str, dest='list_file',
                        default='list.txt', help='Text file with list of sim stats. Format per line:\n\
                            geo_file.geo, wavelength, fibre, reemis, abs')
    parser.add_argument('---step', '-s', type=str, dest='step',
                    default='all', choices=['sim', 'hist', 'sim-hist', 'FOM', 'slope', 'hist-FOM',
                                            'hist-Slopes', 'allFOM', 'allSlope'],
                    help='which step of the process is it in?')
    parser.add_argument('---verbose', '-v', type=bool, dest='verbose',
                    default=False, help='print and save extra info')

    args = parser.parse_args()
    return args


def checkRepo(repo_address, verbose=False):
    '''Check format of repo address is right to be used here. Also if repo does not exist, create it.'''

    # Format address
    new_address = repo_address
    if new_address == '.':
        new_address = ''
    elif new_address != '':
        if (new_address[int(len(new_address) - 1)] != '/'):
            new_address += '/'
        
        # If directory does not exists, make it
        if not os.path.isdir(new_address):
            os.mkdir(new_address)
            if verbose:
                print('Created new directory: ', new_address)

    return new_address


def makeMacros(example_macro, save_macro_folder, save_sims_folder, abs_nominal, geo_file, wavelength, fibre, reemis, abs, nevts):
    '''Make and save macro according to provided parameters'''

    # Modify by factor abs_scaling_fact
    abs_scaling_fact = float(abs)
    abs_scaling = '[' + str(abs_nominal[0] * abs_scaling_fact)
    abs_scaling += ', ' + str(abs_nominal[1] * abs_scaling_fact)
    abs_scaling += ', ' + str(abs_nominal[2] * abs_scaling_fact)
    abs_scaling += ', ' + str(abs_nominal[3] * abs_scaling_fact) + ']'

    # AMELLIE_geoFile_LEDnum_fibre_reemis_abs.root
    new_macro_address = save_macro_folder + 'AMELLIE' + geo_file[:-4] + '_' + wavelength + '_' + fibre + 'reemis' + reemis + '_abs' + str(abs_scaling_fact) + '.mac'
    output_address = save_sims_folder + 'AMELLIE' + geo_file[:-4] + '_' + wavelength + '_' + fibre + 'reemis' + reemis + '_abs' + str(abs_scaling_fact) + '.root'

    new_macro = []
    for line in example_macro:
        # Replace placeholders in macro
        if 'geo/YOUR_FAV_GEO_FILE.geo' in line:
            new_line = line.replace('YOUR_FAV_GEO_FILE.geo', geo_file, 1)
        elif 'YOUR_FAV_LED' in line:
            new_line = line.replace('YOUR_FAV_LED', wavelength, 1)
        elif 'YOUR_FAV_FIBRE' in line:
            new_line = line.replace('YOUR_FAV_FIBRE', fibre, 1)
        elif '/PATH/TO/YOUR/OUTPUT/FILE.root' in line:
            new_line = line.replace('/PATH/TO/YOUR/OUTPUT/FILE.root', output_address, 1)
        elif '/rat/run/start 2000' in line:
            new_line = line.replace('/rat/run/start 2000', '/rat/run/start ' + args.nevts, 1)
        elif 'ABSLENGTH_SCALING [1.9, 2.0, 2.0, 0.0]' in line:
            new_line = line.replace('[1.9, 2.0, 2.0, 0.0]', abs_scaling, 1)
        else:
            new_line = line

        new_macro.append(new_line)

    return new_macro_address


def runSims(args, input_info):
    '''Runs AMELLIE simulations based in input information'''

    # Read in example macro
    repo_address = __file__[:-len('scripts/runAMMELIE.py')]
    macro_address = repo_address + 'macros/runSimulation.mac'
    with open(macro_address, "r") as f:
        example_macro = f.readlines()

    # Make sure folders are of the correct format to  use later
    save_macro_folder = checkRepo(args.macro_repo, args.verbose)
    save_sims_folder = checkRepo(args.sim_repo, args.verbose)

    # Nominal ABSLENGTH_SCALING (see rat/data/OPTICS_TeDiol_0p5_bismsb_dda.ratdb)
    abs_nominal = [0.95, 1.0, 1.0, 2.3]  #(fyi for ABSLENGTH_SCALING: 0 = LAB, 1 = PPO, 2 = Te-Diol, 3 = bisMSB)
    
    macro_addresses = []
    for geo_file, wavelength, fibre, reemis, abs in input_info:
        # Create all the macros
        new_macro_address = makeMacros(example_macro, save_macro_folder, save_sims_folder, abs_nominal, geo_file, wavelength, fibre, reemis, abs, args.nevts)
        macro_addresses.append(new_macro_address)

        # Create all the job scripts
        

        # # Run new macro
        # random_seed = '-1829418327'  # option to make sure all sims have the same random seed, to isolate changes
        # command = 'rat ' + new_macro_address #+ ' -s ' + random_seed
        # print(command)
        # subprocess.call(command, stdout=subprocess.PIPE, shell=True) # use subprocess to make code wait until it has finished



    return new_address


def main():
    # read in argument
    args = argparser()

    # read in file info
    textFile = open(args.list_file, "r")
    lines = [line.split(', ') for line in textFile]
    textFile.close()
    input_info = np.asarray(lines)

    # geo_files = input_info[:, 0]
    # wavelengths = input_info[:, 1]
    # fibres = input_info[:, 2]
    # reemissions = input_info[:, 3]
    # abs_factors = input_info[:, 4]

    work_modes = {
        'sim': runSims,
        'hist': getHists,
        'sim-hist': runSims_getHists,
        'FOM': getFOMs,
        'slope': getSlopes,
        'hist-FOM': getHists_FOMs,
        'hist-Slopes': getHists_Slopes,
        'allFOM': runAllFOMs,
        'allSlope': runAllSlopes
    }

    result = work_modes[args.step](args, input_info)


if __name__ == '__main__':
    main()