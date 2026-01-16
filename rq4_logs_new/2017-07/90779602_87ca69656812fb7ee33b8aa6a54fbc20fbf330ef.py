import pandas as pd
import csv
import numpy as np
import re

import os
import fnmatch

# Capture all subject lang files
# Skip spreadsheets that have errors for missing tabs, or ill formatted headers
# Create a seperate list for each error with failed subjects/files

work_dir = '/Users/axs97/Desktop/lang_eval_to_redcap-alexs'

os.chdir(work_dir)


def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

lang_files = find('*.xls', work_dir + '/Patients/')
# lang_files = [work_dir +
# '/Patients/LastNameA_F/Adamian_Daniel
# /010815/adamian_lang_010815.xls']

data = []

# cols will be used to build dataframe off of specific Redcap headers
cols = pd.read_csv(work_dir + '/redcap_headers.csv')

single_test = pd.DataFrame()
count = 0

missing_bnt30 = []
missing_wab_commands = []
missing_wab_repetition = []
missing_wab_reading = []

header_error_bnt30 = []
header_error_wab_reading = []

missing_transcr = []
transcr_response_error = []

all_test = pd.DataFrame()

for file in lang_files:  # Iterate through every found excel file
    single_test = pd.DataFrame()

    # Find subject's name from file path
    single_test['Subject'] = []
    m = re.search(work_dir + '/Patients/LastNameA_F/(.+?)/', file)
    if m:
        found = m.group(1)
    m = re.search(work_dir + '/Patients/LastNameG_M/(.+?)/', file)
    if m:
        found = m.group(1)
    m = re.search(work_dir + '/Patients/LastNameN_Z/(.+?)/', file)
    if m:
        found = m.group(1)
    single_test.ix[0, 'Subject'] = found

    xl = pd.ExcelFile(file)
    sprdshts = xl.sheet_names  # see all sheet names

    # Boston Naming Test 30
    if 'BNT30' in sprdshts:
        bnt30 = pd.read_excel(file, 'BNT30')
        headers = bnt30.loc[3].tolist()
        headers[0] = 'item'

        relevant_headers = [
            'Spont correct (1, 0)',
            'Verbatim response if incorrect',
            'Spont gesture if given (1, 0)',
            'Correct w/sem cue (1,0)',
            'Verbatim response if incorrect after stim cue',
            'Correct w/ph cue (1,0)',
            'Verbatim response if incorrect after ph cue',
            'Correct w/mult choice (1,0)',
            'Response if incorrect'
            ]

        temp_head_errors = []

        for head in relevant_headers:
            if head in headers:
                continue
            else:
                temp_head_errors.append(head)
        if not temp_head_errors:
            bnt30_notNaN = bnt30[~pd.isnull(bnt30['Boston Naming Test'])]
            bnt30_only_items = bnt30_notNaN[pd.isnull(bnt30
                                                      ['Boston Naming Test']
                                                      .str.isnumeric())]
            bnt30_only_items.columns = headers

            bnt30_relevant = bnt30_only_items[[
                'Spont correct (1, 0)',
                'Verbatim response if incorrect',
                'Spont gesture if given (1, 0)',
                'Correct w/sem cue (1,0)',
                'Verbatim response if incorrect after stim cue',
                'Correct w/ph cue (1,0)',
                'Verbatim response if incorrect after ph cue',
                'Correct w/mult choice (1,0)',
                'Mult choice prompts',
                'Response if incorrect'
                ]]

            # items = bnt30_only_items[NaN].loc()
            bnt30_relevant = bnt30_relevant.set_index(bnt30_only_items['item'])
            bnt30_relevant.insert(5, 'notes', "")

            items = bnt30_relevant.index.tolist()
            for i in items:
                temp_list = ['', '', '', '', '', '', '', '']
                if i < 10:
                    # replace first value with correct string
                    test = 'bnt30_response_' + str(i)
                    if bnt30_relevant.loc[i]['Spont correct (1, 0)'] == 1:
                        temp_list[0] = 'Spontaneous'
                    elif bnt30_relevant.loc[i]['Correct w/sem cue (1,0)'] == 1:
                        temp_list[0] = 'Semantic cue'
                    elif bnt30_relevant.loc[i]['Correct w/ph cue (1,0)'] == 1:
                        temp_list[0] = 'Phonemic cue'
                    else:
                        temp_list[0] = 'Not named'

                    if bnt30_relevant.loc[i]['Spont correct (1, 0)'] == 0:
                        temp_list[1] = (bnt30_relevant.loc[i]
                                        ['Verbatim response if incorrect'])
                        temp_list[2] = (bnt30_relevant.loc[i]
                                        ['Spont gesture if given (1, 0)'])

                    if bnt30_relevant.loc[i]['Correct w/sem cue (1,0)'] == 0:
                        temp_list[3] = (bnt30_relevant.loc[i]
                                        ['Verbatim response if '
                                         'incorrect after stim cue'])

                    if bnt30_relevant.loc[i]['Correct w/ph cue (1,0)'] == 0:
                        temp_list[4] = (bnt30_relevant.loc[i]
                                        ['Verbatim response '
                                         'if incorrect after ph cue'])

                    if bnt30_relevant.loc[i]['Correct w/mult '
                                             'choice (1,0)'] == 1:
                        temp_list[6] = 'Correct'
                    elif bnt30_relevant.loc[i]['Correct w/mult '
                                               'choice (1,0)'] == 0:
                        temp_list[6] = 'Incorrect'
                        if bnt30_relevant.loc[i]['Response '
                                                 'if incorrect'] != '':
                            temp_list[7] = (bnt30_relevant.loc[i]
                                            ['Response if incorrect'])
                        else:
                            temp_list[7] = 'Check excel sprdsht'

                    temp_df = pd.DataFrame([temp_list],
                                           columns=[col for col in cols.columns
                                                    if 'bnt30' in col and
                                                    '_'+str(i) in col[-2:]])
                else:
                    if bnt30_relevant.loc[i]['Spont correct (1, 0)'] == 1:
                        temp_list[0] = 'Spontaneous'
                    elif bnt30_relevant.loc[i]['Correct w/sem cue (1,0)'] == 1:
                        temp_list[0] = 'Semantic cue'
                    elif bnt30_relevant.loc[i]['Correct w/ph cue (1,0)'] == 1:
                        temp_list[0] = 'Phonemic cue'
                    else:
                        temp_list[0] = 'Not named'

                    if bnt30_relevant.loc[i]['Spont correct (1, 0)'] == 0:
                        temp_list[1] = (bnt30_relevant.loc[i]
                                        ['Verbatim response if incorrect'])
                        temp_list[2] = (bnt30_relevant.loc[i]
                                        ['Spont gesture if given (1, 0)'])

                    if bnt30_relevant.loc[i]['Correct w/sem cue (1,0)'] == 0:
                        temp_list[3] = (bnt30_relevant.loc[i]
                                        ['Verbatim response if '
                                         'incorrect after stim cue'])

                    if bnt30_relevant.loc[i]['Correct w/ph cue (1,0)'] == 0:
                        temp_list[4] = (bnt30_relevant.loc[i]
                                        ['Verbatim response '
                                         'if incorrect after ph cue'])

                    if bnt30_relevant.loc[i]['Correct w/mult '
                                             'choice (1,0)'] == 1:
                        temp_list[6] = 'Correct'
                    elif bnt30_relevant.loc[i]['Correct w/mult '
                                               'choice (1,0)'] == 0:
                        temp_list[6] = 'Incorrect'
                        if (bnt30_relevant.loc[i]
                                ['Response if incorrect']) != '':
                            temp_list[7] = (bnt30_relevant.loc[i]
                                            ['Response if incorrect'])
                        else:
                            temp_list[7] = 'Check excel sprdsht'

                    temp_df = pd.DataFrame([temp_list],
                                           columns=[col for col in
                                                    cols.columns if 'bnt30' in
                                                    col and '_' + str(i)
                                                    in col])
                single_test = pd.concat([single_test, temp_df], axis=1)
        else:
            header_error_bnt30.append([file, temp_head_errors])
    else:
        missing_bnt30.append(file)
    all_test = all_test.append(single_test)
all_test.to_csv('BNT30-Final.csv', encoding='utf-8')