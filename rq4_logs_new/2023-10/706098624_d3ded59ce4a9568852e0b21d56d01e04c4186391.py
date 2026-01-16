# -*- coding: utf-8 -*-

#
# Import Modules
#

import argparse
import logging
import numpy as np
import os
import pandas as pd
import sys
import yaml


#
# Local Functions
#


# Local functions
def getNM(repdf, lowL='pdm#LEVEL', highL='p#LEVEL', scanfreq='ScanFreq#REL', nmLabels={'a#REL': ['U']}):
    '''
    Overview: Get the non modified peptidoform used as a reference
    in each cluster

    Input:
        - repdf: Pandas dataframe with report format (multiindex)
        - lowL: String indicating low level
        - highL: String indicating high level
        - nmLabels: Dictionary indicating the criteria used to identify the 
        non modified element.

    Output:
        - repdf: Pandas dataframe with report format.
            - +=
            - (NM, REL) column containing non modified element       
    '''

    idx = pd.IndexSlice

    # working columns
    cols = [lowL, highL, scanfreq] + list(nmLabels.keys())
    cols = [i.split('#') for i in cols]

    #import pdb; pdb.set_trace()

    df = repdf.loc[:, cols].droplevel(1, axis=1).groupby(highL.split('#')[0]).agg(list).reset_index()
    df = df.loc[:,~df.columns.duplicated()]

    # {nmLabel1: [[[label_ijk],...[]n],...,[]m]} where m is the number of clusters, and n is number of pdm in the given cluster
    # label_ijk is the label of pdm i in cluster j for label k
    tmp = {i: df[i.split('#')[0]].to_list() for i in nmLabels.keys()}

    # replace label_ijk for a boolean (True if non-modified)
    tmp = {i: [[k in nmLabels[i] for k in j] for j in tmp[i]] for i in tmp}

    # Collapse boolean of the same pdm --> [[b_1j,... b_ij], ..., []m] where m is the number of clusters, where b_ij is boolean of pdm i in cluster j
    tmp = [np.logical_or.reduce(i).tolist() for i in zip(*[tmp[i] for i in tmp])]

    # Join each pdm with its frequency and index position --> [[(i_1j, b_1j, s_1j), ..., ()n], ..., []m]
    tmp = [list(zip(range(len(i)), i,j)) for i,j in zip(tmp, df[scanfreq.split('#')[0]].to_list())]

    # Remove pdm with false
    tmp = [[j for j in i if j[1]] for i in tmp]

    # Get position of the pdm with biggest frequency
    tmp = [i[np.argmax([j[2] for j in i])][0] if len(i)>0 else -1 for i in tmp]

    # Extract pdm using the position
    nm_list = [j[i] if i!=-1 else "" for i,j in zip(tmp, df[lowL.split('#')[0]].to_list())]

    # Add NM to df
    nmcol = f"{lowL.split('#')[0]}_NM"
    df[nmcol] = nm_list

    # Merge report with non modified using cluster as key
    tmp = df.loc[:, [highL.split('#')[0], nmcol]]
    tmp.columns = pd.MultiIndex.from_product([tmp.columns, ['EXTRA']])

    repdf = pd.merge(
        repdf,
        tmp,
        left_on=[(i, j) for i,j in repdf.columns if i == highL.split('#')[0]][:1],
        right_on=[(highL.split('#')[0], 'EXTRA')],
        how='left'
    ).drop((highL.split('#')[0], 'EXTRA'), axis=1)

    return repdf



def Z_NM(repdf, integration, lowL):
    '''
    Overview: Add each peptidoform the Z values of the NM used as reference
    and calculate delta Z.

    Input:
        - repdf: Pandas dataframe with report format (multiindex)
        - integration: String indicating integration (Z_pgm2p)
        - lowL: String indicating low level

    Output:
        - repdf: Pandas dataframe with report format.
            - +=
            - Z_lowL2highL_NM (one per sample)
            - Z_lowL2highL_dNM (one per sample)
    '''

    idx = pd.IndexSlice
    nmcol = f"{lowL.split('#')[0]}_NM"

    repdf_nm = repdf.loc[:, [lowL.split('#'), [nmcol, 'EXTRA']]]

    repdf_nm = repdf_nm.join(repdf.loc[:, idx[[integration], :]])

    repdf_nm = repdf_nm.drop_duplicates().set_index((lowL.split('#')[0], lowL.split('#')[1]))
    
    repdf_nm = repdf_nm.loc[repdf_nm[nmcol, 'EXTRA'].drop_duplicates()[repdf_nm[nmcol, 'EXTRA'].drop_duplicates()!=''],:
        ].reset_index().drop([(lowL.split('#')[0], lowL.split('#')[1])], axis=1).rename(columns={integration:integration+'_NM'})

    repdf = pd.merge(
        repdf,

        repdf_nm,

        on=[(nmcol, 'EXTRA')],
        how='left'
    )

    tmp = repdf.loc[:, idx[integration]] - repdf.loc[:, idx[integration+'_NM']]
    tmp.columns = pd.MultiIndex.from_product([[integration+'_dNM'], tmp.columns])
    repdf = repdf.join(tmp)

    # Drop column containing Z of non modified (we only maintain Z_difference)
    repdf = repdf.drop(integration+'_NM', axis=1, level=0)

    return repdf


def Z_noNM(repdf, integration, lowL):
    '''
    Parameters
    ----------
    repdf : Pandas DataFrame
        DESCRIPTION.
    integration : string
        DESCRIPTION.

    Returns
    -------
    Pandas DataFrame.

    '''
    nmcol = f"{lowL.split('#')[0]}_NM"
    integration_d = integration+'_dNM'
    idx = pd.IndexSlice
    bna = (repdf[nmcol, 'EXTRA']=='').to_list()
    #bna = pd.isna(repdf.loc[:, idx[integration_d,:]]).all(axis=1)
    cols = pd.MultiIndex.from_tuples([(i,j) for i,j in repdf.columns if i == integration_d])
    
    
    df = pd.DataFrame([
     i if k else j
     for i,j,k in zip(
             zip(*[j for i,j in repdf.loc[:, integration].to_dict('list').items()]),
             zip(*[j for i,j in repdf.loc[:, integration_d].to_dict('list').items()]),
             bna
             )
    ], columns=cols)
    
    repdf = repdf.drop(cols, axis=1).join(df)
    
    repdf['info-'+integration+'_dNM', 'EXTRA'] = ['psp' if i else 'sp' for i in bna]
    
    # Remove 0 of NM
    repdf = repdf.drop(cols, axis=1).join(repdf.loc[:, cols].replace(0, np.nan))
    
    return repdf
    
    


#
# Main
# 

def main(config, file=None):
    '''
    main
    '''
    
    # read report
    logging.info(f"Reading report: {config['infile']}")
    if file==None:
        repdf = pd.read_csv(config['infile'], sep='\t', header=[0,1], low_memory=False)
    else:
        repdf = file

    # get nm
    logging.info(f"Searching non modified element")
    nmLabels = {}
    _ = [nmLabels.update(i) for i in config['NM_columns']]
    repdf = getNM(repdf, lowL=config['low_level'], highL=config['high_level'], scanfreq=config['scanfreq'], nmLabels=nmLabels)

    # calculate delta Z
    logging.info(f"Calculating dZ")
    repdf = Z_NM(repdf, config['integration'], config['low_level'])
    
    # if dZ is NaN because of NM absence, get original Z
    logging.info(f"Get Z of elements without NM reference")
    repdf = Z_noNM(repdf, config['integration'], config['low_level'])
    
    # Replace 0 by nan
    #repdf.loc[:, config['integration']+'_dNM'].replace(0, np.nan, inplace=True)
    repdf.replace({config['integration']+'_dNM':{0:np.nan}}, inplace=True)
    
    


    # write report
    outpath = os.path.join(os.path.dirname(config['infile']), 'NM_'+os.path.basename(config['infile']))
    logging.info(f"Writing output report: {outpath}")
    repdf.to_csv(
        outpath,
        sep='\t',
        index=False
    )

    return repdf


if __name__ == '__main__':
    

    parser = argparse.ArgumentParser(
        description='NMpyCompare',
        epilog='''
        Example:
            python NMpyCompare.py
        ''')

    parser.add_argument('-c', '--config', default=os.path.join(os.path.dirname(__file__), 'NMpyCompare.yaml'), type=str, help='Path to config file')

    args = parser.parse_args()


    with open(args.config) as file:
        config = yaml.load(file, yaml.FullLoader)
        

    logging.basicConfig(level=logging.INFO,
                        format='NMpyCompare.py - '+str(os.getpid())+' - %(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        handlers=[logging.FileHandler(
                            os.path.splitext(config['infile'])[0]+'.log'
                            ),
                            logging.StreamHandler()])

    logging.info('Start script: '+"{0}".format(" ".join([x for x in sys.argv])))
    repdf = main(config)
    logging.info(f'End script')
