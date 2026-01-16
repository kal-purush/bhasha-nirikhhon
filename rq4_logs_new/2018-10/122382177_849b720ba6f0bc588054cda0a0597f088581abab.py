import argparse, csv, os, webbrowser, datetime, glob, subprocess

# generate date month/day/year string
mm_dd_yy = datetime.datetime.now().strftime("%m%d%y")

# set working dir to ccdg woid dir when deployed
# working_dir = os.getcwd()
# working_dir = '/gscmnt/gc2783/qc/topmed_assay'
working_dir = '/Users/ltrani/Desktop/git/qc/ccdg_launcher/topmed_test'
os.chdir(working_dir)

qc_fieldnames = ['WOID','QC Sample','PSE','# of Inputs','# of Instrument Data','LIMS Status','Instrument Check',
                 'Launch Status','Launch Date','QC Status','QC Date','QC Failed Metrics','COD Collaborator',
                 'QC Directory','Top Up']


def main():
    desc_str = """
        Program to create sample emails and update sample launch status in tracking spreadsheets.
    """

    parser = argparse.ArgumentParser(description=desc_str)

    group_a = parser.add_mutually_exclusive_group()

    group_a.add_argument("-f", type=str, help='Input compute workflow file with all samples')
    group_a.add_argument('-l', help='Link to links compute workflow file', action='store_true')
    group_a.add_argument('-t', help='Update status file with topup samples', action='store_true')
    group_a.add_argument('-a', help='add additional samples to status file', action='store_true')

    group_b = parser.add_mutually_exclusive_group()
    group_b.add_argument("-w", type=str, help='check failed samples, enter woids as comma separated list or all for '
                                              'everything')
    args = parser.parse_args()

    if args.l:
        generate_compute_workflow()
        quit()

    if args.t:
        woid_dirs = woid_list()
        ccdg_launcher('args.t')

    if args.a:
        sample_add()

    if args.w and args.f:
        if not os.path.exists(args.f):
            print('{} file not found.'.format(args.f))

        if os.path.exists(args.f):
            fail_files_check(args.w,args.f)

    if args.f and not args.w:
        if not os.path.exists(args.f):
            print('{} file not found.'.format(args.f))

        if os.path.exists(args.f):
            woid_dirs = woid_list()
            ccdg_launcher(working_dir + '/' + args.f)


def fail_files_check(args_in, in_file):

    woid_dirs = []

    outfile = 'tm.fail.recheck.' + mm_dd_yy + '.tsv'
    infile = os.getcwd() + '/' + in_file

    if args_in == 'all':
        woid_dirs = woid_list()
    else:
        woid_dirs = args_in.split(',')

    with open(outfile, 'w') as outfilecsv:
        outfile_writer = csv.writer(outfilecsv, delimiter='\n')

        for woid in woid_dirs:
            os.chdir(working_dir)

            sample_list = []

            if not os.path.exists(woid):
                print('\n{} dir does not exist.\nExiting ccdgl.'.format(woid))
                exit()

            qc_status_file = woid + '.qcstatus.tsv'
            launch_failed_file = woid + '.launch.fail.tsv'
            instrument_pass_status_active_file = woid + '.instrument.pass.status.active.tsv'

            os.chdir(woid)

            if os.path.exists(launch_failed_file) and os.path.exists(instrument_pass_status_active_file) and \
                    os.path.exists(qc_status_file):

                print('-------------------------------------------')
                outfile_writer.writerow(['-------------------------------------------'])
                print('-------\n{}\n-------'.format(woid))
                outfile_writer.writerow(['-------'])
                outfile_writer.writerow([woid])
                outfile_writer.writerow(['-------'])

                with open(launch_failed_file, 'r') as lfcsv, open(instrument_pass_status_active_file, 'r') as ipsafcsv:
                    lfcsv_reader =csv.DictReader(lfcsv, delimiter='\t')
                    ipsafcsv_reader = csv.DictReader(ipsafcsv, delimiter='\t')

                    for line in lfcsv_reader:
                        if line['QC Sample'][0] == '0':
                            line['QC Sample'] = line['QC Sample'][1:]
                        sample_list.append(line['QC Sample'])

                    for line in ipsafcsv_reader:
                        if line['QC Sample'][0] == '0':
                            line['QC Sample'] = line['QC Sample'][1:]
                        sample_list.append(line['QC Sample'])

                # create compute workflow file
                woid_cw_file = compute_workflow_create(infile, woid)

                # check to see if samples exist in compute workflow file
                samples_not_found_in_cw = []
                for sample in sample_list:
                    samples_not_found_in_cw = cw_sample_check(woid_cw_file, sample)

                if len(samples_not_found_in_cw) > 0:
                    print('There are launch samples not found in {}:\n'.format(woid_cw_file))
                    for sample in samples_not_found_in_cw:
                        print(sample)
                    quit()

                # run qc status updates on samples
                if os.path.exists(qc_status_file) and os.path.exists(woid_cw_file) and os.path.exists(
                        launch_failed_file):
                    output = qc_status_update(woid_cw_file, sample_list, woid, qc_status_file)

                    for line in output:
                        outfile_writer.writerow([line])
                outfile_writer.writerow('')

                subprocess.run(["/gscuser/awagner/bin/python3", "/gscuser/awollam/aw/ccdg_zero_restore.py",
                                "-w", woid])

                os.chdir(working_dir)
    quit()
    return


# print link to download compute workflow from lims and cat command
def generate_compute_workflow():

    firefox_path = '/gapp/ia32linux/bin/firefox %s'
    launch_link = 'https://imp-lims.gsc.wustl.edu/entity/compute-workflow?_show_result_set_definition=1&_result_set_' \
                  'name=&cw_id=&creation_event_id=&assay_id=&assay_id=&protocol_id=761&date_scheduled=&sample_full_' \
                  'name=&number_of_input=&number_of_instrument_data='
    open_url = input('Open compute workflow in firefox? (y/n)\n')
    while open_url not in ('y', 'n'):
        open_url = input('Please enter y or n:\n')
    if open_url == 'y':
        webbrowser.get(firefox_path).open(launch_link)

    print('\nCompute workflow link:\n{}'.format(launch_link))
    print('\nCreate workflow file:\ncat > tm.computeworkflow.{}.tsv\n'.format(mm_dd_yy))


# make list of all woid dirs, filter out anything that isn't a woid
def woid_list():

    woid_dirs = []

    def is_int(string):

        try:
            int(string)
        except ValueError:
            return False
        else:
            return True

    woid_dir_unfiltered = glob.glob('285*')
    for woid in filter(is_int, woid_dir_unfiltered):
        woid_dirs.append(woid)
    return woid_dirs


# create computeworkflow file in woid directory
def compute_workflow_create(compute_workflow_all_file, woid):

    if not os.path.exists(working_dir + '/' + woid):
        os.chdir(woid)

    outfile = woid + '.compute.workflow.' + mm_dd_yy + '.tsv'
    with open(compute_workflow_all_file, 'r') as compute_workflow_all_csv, open(outfile, 'w') as outfilecsv:

        cwl_reader = csv.DictReader(compute_workflow_all_csv, delimiter='\t')
        cwl_fieldnames = cwl_reader.fieldnames

        outfile_writer = csv.DictWriter(outfilecsv, fieldnames=cwl_fieldnames, delimiter='\t')
        outfile_writer.writeheader()

        for line in cwl_reader:
            if (woid in line['Work Order']) and 'Aligned Bam To BQSR Cram And VCF' in line['Protocol']:
                outfile_writer.writerow(line)
    return outfile


# Check compute workflow file to see if all samples are there
sample_not_found = []
def cw_sample_check(infile, sample):

    cw_samples = []
    with open(infile) as infiletsv:
        infile_reader = csv.DictReader(infiletsv, delimiter='\t')
        for qc_data in infile_reader:
            cw_samples.append(qc_data['Sample Full Name'])

    if sample not in cw_samples:
        sample_not_found.append(sample)
    return sample_not_found


# assing qc fields, check instrument match and sample lims status
def sample_pse_match(infile, sample, woid):

    qc_results = {}
    if str(sample[0]) == '0':
        sample = sample[1:]

    with open(infile) as infiletsv:

        infile_reader = csv.DictReader(infiletsv, delimiter='\t')

        for qc_data in infile_reader:
            if sample in qc_data['Sample Full Name']:
                if sample == qc_data['Sample Full Name']:
                    qc_results['QC Sample'] = qc_data['Sample Full Name']
                    qc_results['PSE'] = qc_data['PSE']
                    qc_results['# of Inputs'] = qc_data['# of Inputs']
                    qc_results['# of Instrument Data'] = qc_data['# of Instrument Data']
                    qc_results['Launch Date'] = mm_dd_yy
                    qc_results['WOID'] = woid
                    qc_results['QC Status'] = 'NONE'
                    qc_results['QC Date'] = 'NONE'
                    qc_results['QC Failed Metrics'] = 'NONE'
                    qc_results['COD Collaborator'] = 'NONE'
                    qc_results['QC Directory'] = 'NONE'
                    qc_results['LIMS Status'] = qc_data['Status']

                    if (int(qc_data['# of Inputs']) == int(qc_data['# of Instrument Data'])) and qc_data['Status'] == \
                            'ready':
                        qc_results['Instrument Check'] = 'PASS'
                        qc_results['Launch Status'] = 'Launched'

                    elif (int(qc_data['# of Inputs']) == int(qc_data['# of Instrument Data'])) and qc_data['Status'] ==\
                            'active':
                        qc_results['Instrument Check'] = 'PASS'
                        qc_results['Launch Status'] = 'MANUAL LAUNCH'
                        qc_results['Launch Date'] = 'NA'

                    else:
                        qc_results['Instrument Check'] = 'FAIL'
                        qc_results['Launch Status'] = 'DO NOT LAUNCH'
                        qc_results['Launch Date'] = 'NA'

    return qc_results


# print sample status results
def output_launcher_results(pse_ready, pse_fail, pse_fail_ready, pse_fail_fail, ins_pass_stat_actv):

    output = []
    if len(pse_ready) > 0:
        print('\n{} Samples pass instrument check, status updated to Launched:'.format(str(len(pse_ready))))
        output.append(str(len(pse_ready)) + ' Samples pass instrument check, status updated to Launched:')
        print('Sample', 'PSE', sep='\t')
        output.append('Sample\tPSE')
        for pse in pse_ready:
            print(pse)
            output.append(pse)

    if len(pse_fail) > 0:
        print('\n{} Samples failed instrument check, status is do not launch: '.format(str(len(pse_fail))))
        output.append(str(len(pse_fail)) + 'Samples failed instrument check, status is do not launch:')
        print('Sample', 'PSE', '# of Inputs', '# of Instrument Data', sep="\t")
        output.append('Sample\tPSE\t# of Inputs\t# of Instrument Data')
        for pse in pse_fail:
            print(pse)
            output.append(pse)

    if len(pse_fail_ready) > 0:
        print('\n{} Failed samples, pass instrument check, status updated to Launched:'
              .format(str(len(pse_fail_ready))))
        output.append(str(len(pse_fail_ready)) + ' Failed samples, pass instrument check, status updated to Launched:')
        print('Sample', 'PSE', sep='\t')
        output.append('Sample\tPSE')
        for pse in pse_fail_ready:
            print(pse)
            output.append(pse)

    if len(pse_fail_fail) > 0:
        print('\n{} Samples failed instrument check again, status is do not launch:'.format(str(len(pse_fail_fail))))
        output.append(str(len(pse_fail_fail)) + ' Samples failed instrument check again, status is do not launch:')
        for pse in pse_fail_fail:
            print(pse)
            output.append(pse)

    if len(ins_pass_stat_actv) > 0:
        print('\n{} Samples pass instrument check, protocol status is active:'.format(str(len(ins_pass_stat_actv))))
        output.append(str(len(ins_pass_stat_actv)) + ' Samples pass instrument check, protocol status is active:')
        print('Sample', 'PSE', '# of Inputs', '# of Instrument Data', 'LIMS Status', sep="\t")
        output.append('Sample\tPSE\t# of Inputs\t# of Instrument Data\tLIMS Status')
        for pse in ins_pass_stat_actv:
            print(pse)
            output.append(pse)

        print('Manually launch samples if they have not already been launched (check jira).')
        output.append('Manually launch samples if they have not already been launched (check jira).')

    else:
        print('\nAll processed samples have a launch status.')
        output.append('All processed samples have a launch status.')

    return output


# process samples update qc files and populate results lists for printing
qc_master_sample_list = []
def qc_status_update(compute_workflow, sample_list, woid, qc_status_file):

    temp_status_file = woid + '.qcstatus.temp.tsv'
    launch_failed_temp = woid + '.launch.fail.temp.tsv'
    launch_failed_file = woid + '.launch.fail.tsv'
    instrument_pass_status_active_file = woid + '.instrument.pass.status.active.tsv'
    instrument_pass_status_active_file_tmp = woid + '.instrument.pass.status.active.temp.tsv'

    with open(qc_status_file,'r') as qcstatuscsv, open(temp_status_file,'w') as tempstatuscsv, \
            open(launch_failed_temp,'w') as launchtempcsv, open(instrument_pass_status_active_file_tmp,'w') as ipsafcsv:

        status_file_reader = csv.DictReader(qcstatuscsv, delimiter='\t')
        status_file_header = status_file_reader.fieldnames

        temp_status_writer = csv.DictWriter(tempstatuscsv, fieldnames=status_file_header, delimiter='\t')
        temp_status_writer.writeheader()

        temp_launch_fail_writer = csv.DictWriter(launchtempcsv, fieldnames=status_file_header, delimiter='\t')
        temp_launch_fail_writer.writeheader()

        instrument_pass_status_active_writer = csv.DictWriter(ipsafcsv, fieldnames=status_file_header, delimiter='\t')
        instrument_pass_status_active_writer.writeheader()

        # Declare lists for print statements to terminal
        pse_ready = []
        pse_fail = []
        pse_fail_ready = []
        pse_fail_fail = []
        ins_pass_stat_actv = []

        for line in status_file_reader:
            if line['Full Name'][0] == '0':
                line['Full Name'] = line['Full Name'][1:]
            qc_master_sample_list.append(line['Full Name'])

            # check samples that have already failed
            if (line['Instrument Check'] == 'FAIL' or line['Launch Status'] == 'MANUAL LAUNCH') \
                    and line['Top Up'] == 'NO':
                fail_metrics = sample_pse_match(compute_workflow, line['Full Name'], woid)
                fail_qc_update = dict(list(line.items())+list(fail_metrics.items()))

                # write passed samples to temp file, populate ready list
                if fail_metrics['Instrument Check'] == 'PASS' and fail_metrics['Launch Status'] == 'Launched':
                    temp_status_writer.writerow(fail_qc_update)
                    pse_fail_ready.append(fail_metrics['QC Sample'] + '\t' + fail_metrics['PSE'])

                # write failed samples to temp file, populate not ready list
                if fail_metrics['Instrument Check'] == 'FAIL':
                    temp_status_writer.writerow(fail_qc_update)
                    temp_launch_fail_writer.writerow(fail_qc_update)
                    pse_fail_fail.append(fail_metrics['QC Sample'] + "\t" + fail_metrics['PSE'] + "\t" +
                                         fail_metrics['# of Inputs'] + "\t" + fail_metrics['# of Instrument Data'])

                if (fail_metrics['Instrument Check'] == 'PASS') and (fail_metrics['Launch Status'] == 'MANUAL LAUNCH'):
                    temp_status_writer.writerow(fail_qc_update)
                    instrument_pass_status_active_writer.writerow(fail_qc_update)
                    ins_pass_stat_actv.append(fail_metrics['QC Sample'] + "\t" + fail_metrics['PSE'] + "\t" +
                                              fail_metrics['# of Inputs'] + "\t" + fail_metrics['# of Instrument Data']
                                              + '\t' + fail_metrics['LIMS Status'])

            # check sample status from sample email sent to be qc'd
            elif (line['Full Name'] in sample_list) and (line['Top Up'] == 'NO') and not line['QC Sample']:
                sample = line['Full Name']
                qc_metrics = sample_pse_match(compute_workflow, sample, woid)
                master_qc_update = dict(list(line.items())+list(qc_metrics.items()))

                # write passed samples to temp file
                if qc_metrics['Instrument Check'] == 'PASS' and qc_metrics['Launch Status'] == 'Launched':
                    temp_status_writer.writerow(master_qc_update)
                    pse_ready.append(qc_metrics['QC Sample'] + '\t' + qc_metrics['PSE'])

                # write failed samples to temp file
                if qc_metrics['Instrument Check'] == 'FAIL':
                    temp_status_writer.writerow(master_qc_update)
                    temp_launch_fail_writer.writerow(master_qc_update)
                    pse_fail.append(qc_metrics['QC Sample'] + "\t" + qc_metrics['PSE'] + "\t"
                                    + qc_metrics['# of Inputs'] + "\t" + qc_metrics['# of Instrument Data'])

                if (qc_metrics['Instrument Check'] == 'PASS') and (qc_metrics['Launch Status'] == 'MANUAL LAUNCH'):
                    temp_status_writer.writerow(master_qc_update)
                    instrument_pass_status_active_writer.writerow(master_qc_update)
                    ins_pass_stat_actv.append(qc_metrics['QC Sample'] + "\t" + qc_metrics['PSE'] + "\t" +
                                              qc_metrics['# of Inputs'] + "\t" + qc_metrics['# of Instrument Data'] +
                                              '\t' + qc_metrics['LIMS Status'])

            # skip samples already checked to launch
            else:
                if line['Top Up'] == 'YES':
                    print('Skipping topup sample: {}'.format(line['Full Name']))
                elif line['Full Name'] in sample_list:
                    print('Ignoring previously launched sample: {}\t{}\t{}'
                          .format(line['Full Name'], line['Launch Date'], line['Launch Status']))

                # write all other unchecked samples to file
                temp_status_writer.writerow(line)

        # check to see if sample exists in status file
        for samp in sample_list:

            if samp[0] == '0':
                samp = samp[1:]

            if not samp in qc_master_sample_list:
                print('{} not found in {}'.format(samp, qc_status_file))

    os.rename(temp_status_file, qc_status_file)
    os.rename(launch_failed_temp, launch_failed_file)
    os.rename(instrument_pass_status_active_file_tmp, instrument_pass_status_active_file)

    output = output_launcher_results(pse_ready, pse_fail, pse_fail_ready, pse_fail_fail, ins_pass_stat_actv)

    return output


# Determine sample topup status
def topup_csv_update(topup_samples, woid):

    with open(woid + '.qcstatus.tsv', 'r') as qcstatuscsv, open(woid + '.qcstatus.temp.tsv', 'w') as tempstatuscsv:

        status_reader = csv.DictReader(qcstatuscsv, delimiter='\t')
        status_fieldnames = status_reader.fieldnames

        temp_status_writer = csv.DictWriter(tempstatuscsv, fieldnames=status_fieldnames, delimiter='\t')
        temp_status_writer.writeheader()

        for line in status_reader:
            if line['Full Name'] in topup_samples or line['Top Up'] == 'YES':
                line['Top Up'] = 'YES'
            else:
                line['Top Up'] = 'NO'
            temp_status_writer.writerow(line)

    os.rename(woid + '.qcstatus.temp.tsv', woid + '.qcstatus.tsv')

    return


# add samples to qcstatus file after they have been reactivated
def sample_add():

    while True:
        woid = input('----------\nWork order id (enter to exit):\n').strip()
        if len(woid) == 0:
            print('Exiting topmed launcher.')
            break
        try:
            val = int(woid)
        except ValueError:
            print("\nwoid must be a number.")
            continue

        updated_master_outfile = woid + '.updated.master.samples.tsv'
        qc_status_file = woid + '.qcstatus.tsv'

        if not os.path.exists(woid):
            print('\n{} dir does not exist.'.format(woid))
            continue

        os.chdir(woid)

        print('\nCreate {}.updated.master.samples.tsv file:'.format(woid))
        master_sample_link = 'https://imp-lims.gsc.wustl.edu/entity/setup-work-order/' + woid + \
                             '?perspective=Sample&setup_name=' + woid
        print('Master sample link:\n{}\nInput samples:'.format(master_sample_link))

        master_samples = []
        while True:
            master_sample_line = input()
            if master_sample_line:
                master_samples.append(master_sample_line)
            else:
                break

        if len(master_samples) == 0:
            print('No samples inputed.')
            continue
        master_samples[:] = [x for x in master_samples if 'WOI' not in x]

        updated_sample_dict = {}
        updated_sample_list = []
        for line in master_samples:
            if 'Content' in line:
                pass
            else:
                sample = line.split('\t')[1]
                updated_sample_dict[sample] = line
                updated_sample_list.append(sample)

        status_samples = []
        with open(updated_master_outfile, 'w') as mastercsv, open(qc_status_file, 'r') as statuscsv:
            updated_master_write = csv.writer(mastercsv, delimiter='\n')
            updated_master_write.writerows([master_samples])

            status_reader = csv.DictReader(statuscsv, delimiter='\t')
            for line in status_reader:
                status_samples.append(line['DNA'])

        print('Updated master file has {} samples, {} has {} samples.'.format(len(updated_sample_list), qc_status_file,
                                                                              len(status_samples)))

        if bool(set(status_samples).intersection(updated_sample_list)):
            print('{} samples match inputed master samples.'.format(qc_status_file))
        else:
            print('{} file samples do not match inputed master file samples. Please check sample input file.'
                  .format(qc_status_file))
            os.chdir(working_dir)
            continue

        if len(updated_sample_list) < len(status_samples):
            print('Samples in {} file, not in updated master:'.format(qc_status_file))
            sample_diff = list(set(status_samples) - set(updated_sample_list))
            for samp in sample_diff:
                print(samp)

        sample_add_flag = True
        for sample in updated_sample_dict:
            if sample not in status_samples:
                print('Adding {} to {} in {}.'.format(sample, qc_status_file, woid))
                os.rename(qc_status_file, '{}.status.temp.tsv'.format(woid))
                with open(qc_status_file, 'w') as statuscsv, open(updated_master_outfile, 'r') as mastercsv, \
                        open('{}.status.temp.tsv'.format(woid), 'r') as tempcsv:
                    updated_master_reader = csv.DictReader(mastercsv, delimiter='\t')
                    tmp_csv_reader = csv.DictReader(tempcsv, delimiter='\t')
                    header = tmp_csv_reader.fieldnames

                    status_writer = csv.DictWriter(statuscsv, fieldnames=header, delimiter='\t')
                    status_writer.writeheader()

                    for line in tmp_csv_reader:
                        status_writer.writerow(line)

                    for line in updated_master_reader:
                        if sample in line['DNA']:
                            status_writer.writerow(line)

                    sample_add_flag = False

                os.remove('{}.status.temp.tsv'.format(woid))

        if sample_add_flag:
            print('No samples found to add.')

        os.chdir(working_dir)


# sample email function, create new woid dir if it doesn't exist and master spreadsheet if it doesn't exist.
# write emails to email file
def ccdg_launcher(infile):

    while True:

        sample_list = []

        woid = input('----------\nWork order id (enter to exit):\n').strip()
        if len(woid) == 0:
            print('Exiting topmed launcher.')
            break
        try:
            val = int(woid)
        except ValueError:
            print("\nwoid must be a number.")
            continue

        master_outfile = woid + '.master.samples.tsv'
        qc_status_file = woid + '.qcstatus.tsv'
        launch_failed_file = woid + '.launch.fail.tsv'

        # create woid dir if it does not exist
        if not os.path.exists(woid):
            print('\n{} dir does not exist.'.format(woid))
            create_woid = input('Create {} directory? (y or n)\n'.format(woid))
            if create_woid == 'y':
                os.makedirs(woid)
                os.chdir(woid)
                # create master file if it does not exist and also qcstatus file
                create_master = input('\nCreate {}.master.samples.tsv file? (y or n)\n'.format(woid))
                if create_master == 'y':
                    master_sample_link = 'https://imp-lims.gsc.wustl.edu/entity/setup-work-order/' + woid + \
                                         '?perspective=Sample&setup_name=' + woid
                    print('\nMaster sample link:\n{}\nInput samples:'.format(master_sample_link))
                    master_samples = []
                    while True:
                        master_sample_line = input()
                        if master_sample_line:
                            master_samples.append(master_sample_line)
                        else:
                            break

                    master_samples[:] = [x for x in master_samples if 'WOI' not in x]

                    with open(master_outfile, 'w') as mastercsv, open(qc_status_file, 'w') as statuscsv:
                        master_write = csv.writer(mastercsv, delimiter='\n')
                        status_write = csv.writer(statuscsv, delimiter='\n')
                        master_write.writerows([master_samples])
                        status_write.writerows([master_samples])

                    # Add header to qc status file and create failed to launch file with the same header.
                    with open(qc_status_file, 'r') as qcstempcsv, open('qc.status.temp.tsv', 'w') as qcstatusfilecsv, \
                            open(launch_failed_file, 'w') as launchfailedcsv:
                        temp_reader = csv.DictReader(qcstempcsv, delimiter = '\t')

                        temp_header = temp_reader.fieldnames
                        status_header = temp_header + qc_fieldnames

                        status_writer = csv.DictWriter(qcstatusfilecsv, fieldnames=status_header, delimiter='\t')
                        launch_fail_writer = csv.DictWriter(launchfailedcsv, fieldnames=status_header, delimiter='\t')
                        status_writer.writeheader()
                        launch_fail_writer.writeheader()

                        for line in temp_reader:
                            status_writer.writerow(line)

                        os.rename('qc.status.temp.tsv', qc_status_file)

                os.chdir(working_dir)

                if create_master == 'n':
                    pass
            if create_woid == 'n':
                continue

        # check for topup samples
        topup_samples = []
        tu_sample_y_n = input('\nTopup samples? (y or n)\n')
        while True:
            if tu_sample_y_n == 'n':
                break
            if tu_sample_y_n not in ['y', 'n']:
                tu_sample_y_n = input('\nTopup samples? (y or n)\n')
                continue
            if tu_sample_y_n == 'y':
                while True:
                    tu_sample = input('Sample name: (enter to continue)\n' ).strip()
                    if tu_sample:
                        if tu_sample[0] == '0':
                            tu_sample = tu_sample[1:]
                        if tu_sample in open(woid+'/'+qc_status_file).read():
                            topup_samples.append(tu_sample)
                        if tu_sample not in open(woid+'/'+qc_status_file).read():
                            print('{} not found in {} file.'.format(tu_sample,qc_status_file))
                    else:
                        break
                if infile == 'args.t':
                    os.chdir(woid)
                    topup_csv_update(topup_samples, woid)
                    print('Samples have been updated as topup in {}.qcstatus.tsv'.format(woid))
                    for sample in topup_samples:
                        print(sample)
                    quit()
                break
        # create sample email
        while True:
            sample_number = input('\nSample number:\n')
            try:
                val = int(sample_number)
            except ValueError:
                print('Sample number must be a number.')
            else:
                break

        user_date = input('\nUse today\'s date? (y or n)\n')
        if user_date == 'y':
            sample_outfile = sample_number + 'samples.' + mm_dd_yy
        elif user_date == 'n':
            new_date = input('Input date (MMDDYY):\n')
            try:
                val = int(new_date)
            except ValueError:
                print('Date must be a number')
                exit()
            sample_outfile = sample_number + 'samples.' + new_date

        if os.path.exists(woid + '/' + sample_outfile):
            print('{} file already exists.'.format(sample_outfile))
            continue

        while True:
            print('\nProduction samples (from email):')
            sample_info = []
            while True:
                sample_line = input()
                if sample_line:
                    sample_info.append(sample_line)
                else:
                    break

            if len(sample_info) <= 1:
                print('Please include production samples (from email) or add header line.')
                sample_info = []
                continue

            if 'Library' not in sample_info[0]:
                print('Library field not found in header line.')
                sample_info = []
                continue
            else:
                break

        os.chdir(woid)

        # if there are topup add them to qcstatus file, populate with YES or NO
        topup_csv_update(topup_samples, woid)

        # write samples to email file
        with open(sample_outfile, 'w') as sample_outfilecsv:
            sample_write = csv.writer(sample_outfilecsv, delimiter='\n')
            sample_write.writerows([sample_info])

        # from sample file, use Library field to create sample list
        with open(sample_outfile, 'r') as samplecsv:

            sample_reader = csv.DictReader(samplecsv, delimiter='\t')

            for sample_line in sample_reader:
                sample_remove_whitespace = {k.replace(' ', ''): v for k, v in sample_line.items()}
                sample_name = sample_remove_whitespace['Library']
                sample_name_split = sample_name.split('-lib')[0]
                sample = sample_name_split.strip()
                if sample[0] == '0':
                    sample = sample[1:]
                sample_list.append(sample)

        # create compute workflow file
        woid_cw_file = compute_workflow_create(infile, woid)

        # check to see if samples exist in compute workflow file
        samples_not_found_in_cw = []
        for sample in sample_list:
            samples_not_found_in_cw = cw_sample_check(woid_cw_file, sample)

        if len(samples_not_found_in_cw) > 0:
            print('There are launch samples not found in {}:\n'.format(woid_cw_file))
            for sample in samples_not_found_in_cw:
                print(sample)
            quit()

        # run qc status updates on samples
        if os.path.exists(qc_status_file) and os.path.exists(woid_cw_file) and os.path.exists(launch_failed_file):
            qc_status_update(woid_cw_file, sample_list, woid, qc_status_file)

        os.chdir(working_dir)

    return


if __name__ == '__main__':
    main()
