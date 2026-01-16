# @Author 
# Chloe-Agathe Azencott
# chloe-agathe.azencott@mines-paristech.fr
# April 2016

import argparse
import string
import sys

import numpy as np

import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 16})
orange_color = '#d66000'
blue_color = '#005599'


def plot_numf(results_dir, figure_path, num_repeats=10):
    """ Plot cross-validated number of selected features,
    averaged over multiple repeats.

    Parameters
    ----------
    results_dir: path
        Path to the directory containing the results.

    figure_path: path
        Path to the file where to save the plot.

    num_repeats: (int, None)
        Number of repeated experiments.        
    """
    numf_dict = {'nodes_l1logreg': [],
             'lioness_l1logreg': [],
              'regline_l1logreg': []}#, 'sfan_l2logreg': []

    # read number of selected features
    for repeat_idx in range(num_repeats):
        repeat_dir = '%s/repeat%d/results' % (results_dir, repeat_idx)

        # L1-logreg on node weights
        with open('%s/results.txt' % repeat_dir, 'r') as f:
            for line in f:
                ls = line.split('\t')
                if ls[0] == "Number of features selected per fold:":
                    #print ls[1].split()
                    numf_list = [int(x) for x in ls[1].split()]
                    break
            f.close()
        numf_dict['nodes_l1logreg'].extend(numf_list)

        # L1-logreg on edge weights
        for ntwk in ['lioness', 'regline']:
            with open('%s/%s/results.txt' % (repeat_dir, ntwk), 'r') as f:
                for line in f:
                    ls = line.split('\t')
                    if ls[0] == "Number of features selected per fold:":
                        numf_list = [int(x) for x in ls[1].split()]
                        break
                f.close()
            numf_dict['%s_l1logreg' % ntwk].extend(numf_list)

    numf_data = np.array([numf_dict['nodes_l1logreg'],
                          numf_dict['lioness_l1logreg'],
                          numf_dict['regline_l1logreg']])
    numf_data = np.transpose(numf_data)

    # Plot
    plt.figure(figsize=(10, 5))

    bp = plt.boxplot(numf_data)
    plt.setp(bp['boxes'], color=blue_color)
    plt.setp(bp['whiskers'], color=blue_color)
    plt.setp(bp['fliers'], color=blue_color)
    plt.setp(bp['medians'], color=orange_color)

    plt.title('Cross-validated L1-logistic regression over %d repeats' % num_repeats)
    plt.ylabel('Number of features')
    labels = ('Nodes', 'Edges (Lioness)', 'Edges (Regline)')
    plt.xticks(range(1, 4), labels)#, rotation=35)    

    plt.savefig(figure_path, bbox_inches='tight')
    print "Saved number of features to %s" % figure_path


def plot_cixs(results_dir, figure_path, num_repeats=10):
    """ Plot cross-validated consistency indices, averaged over multiple repeats.

    Parameters
    ----------
    results_dir: path
        Path to the directory containing the results.

    figure_path: path
        Path to the file where to save the plot.

    num_repeats: (int, None)
        Number of repeated experiments.        
    """
    # Read consistency indices
    cix_dict = {'nodes_l1logreg': [],
                'lioness_l1logreg': [],
                'regline_l1logreg': []}#, 'sfan_l2logreg': []

    for repeat_idx in range(num_repeats):
        repeat_dir = '%s/repeat%d/results' % (results_dir, repeat_idx)

        # L1-logreg on node weights
        with open('%s/results.txt' % repeat_dir, 'r') as f:
            for line in f:
                ls = line.split('\t')
                if ls[0] == "Stability (Consistency Index):":
                    cix_list = [float(string.lstrip(string.rstrip(x, ",']\n"), ",' [")) \
                                for x in ls[-1].split(",")]
                    break
            f.close()
        cix_dict['nodes_l1logreg'].extend(cix_list)

        # L1-logreg on edge weights
        for ntwk in ['lioness', 'regline']:
            with open('%s/%s/results.txt' % (repeat_dir, ntwk), 'r') as f:
                for line in f:
                    ls = line.split('\t')
                    if ls[0] == "Stability (Consistency Index):":
                        cix_list = [float(string.lstrip(string.rstrip(x, ",']\n"), ",' [")) \
                                    for x in ls[-1].split(",")]
                        break
                f.close()
            cix_dict['%s_l1logreg' % ntwk].extend(cix_list)    

    cix_data = np.array([cix_dict['nodes_l1logreg'],
                         cix_dict['lioness_l1logreg'],
                         cix_dict['regline_l1logreg']])
    cix_data = np.transpose(cix_data)

    # Plot
    plt.figure(figsize=(10, 5))

    bp = plt.boxplot(cix_data)
    plt.setp(bp['boxes'], color=blue_color)
    plt.setp(bp['whiskers'], color=blue_color)
    plt.setp(bp['fliers'], color=blue_color)
    plt.setp(bp['medians'], color=orange_color)

    plt.title('Cross-validated L1-logistic regression over %d repeats' % num_repeats)
    plt.ylabel('Consistency Index')
    labels = ('Nodes', 'Edges (Lioness)', 'Edges (Regline)')
    plt.xticks(range(1, 4), labels)#, rotation=35)
    plt.ylim(0., 1.)
            
    plt.savefig(figure_path, bbox_inches='tight')
    print "Saved consistency indices to %s" % figure_path


def plot_fovs(results_dir, figure_path, num_repeats=10):
    """ Plot cross-validated Fisher overlaps, averaged over multiple repeats.

    Parameters
    ----------
    results_dir: path
        Path to the directory containing the results.

    figure_path: path
        Path to the file where to save the plot.

    num_repeats: (int, None)
        Number of repeated experiments.        
    """
    fov_dict = {'nodes_l1logreg': [],
                'lioness_l1logreg': [],
                'regline_l1logreg': []}#, 'sfan_l2logreg': []

    # read consistency indices
    for repeat_idx in range(num_repeats):
        repeat_dir = '%s/repeat%d/results' % (results_dir, repeat_idx)

        # L1-logreg on node weights
        with open('%s/results.txt' % repeat_dir, 'r') as f:
            for line in f:
                ls = line.split('\t')
                if ls[0] == "Stability (Fisher overlap):":
                    fov_list = [float(string.lstrip(string.rstrip(x, ",']\n"), ",' [")) \
                                for x in ls[-1].split(",")]
                    break
            f.close()
        fov_dict['nodes_l1logreg'].extend(fov_list)

        # L1-logreg on edge weights
        for ntwk in ['lioness', 'regline']:
            with open('%s/%s/results.txt' % (repeat_dir, ntwk), 'r') as f:
                for line in f:
                    ls = line.split('\t')
                    if ls[0] == "Stability (Fisher overlap):":
                        fov_list = [float(string.lstrip(string.rstrip(x, ",']\n"), ",' [")) \
                                    for x in ls[-1].split(",")]
                        break
                f.close()
            fov_dict['%s_l1logreg' % ntwk].extend(fov_list)    
    
    fov_data = np.array([fov_dict['nodes_l1logreg'],
                         fov_dict['lioness_l1logreg'],
                         fov_dict['regline_l1logreg']])
    fov_data = np.transpose(fov_data)

    # Plot
    plt.figure(figsize=(10, 5))

    bp = plt.boxplot(fov_data)
    plt.setp(bp['boxes'], color=blue_color)
    plt.setp(bp['whiskers'], color=blue_color)
    plt.setp(bp['fliers'], color=blue_color)
    plt.setp(bp['medians'], color=orange_color)

    plt.title('Cross-validated L1-logistic regression over %d repeats' % num_repeats)
    plt.ylabel('Fisher Overlap')
    labels = ('Nodes', 'Edges (Lioness)', 'Edges (Regline)')
    plt.xticks(range(1, 4), labels)#, rotation=35)

    plt.savefig(figure_path, bbox_inches='tight')
    print "Saved Fisher overlaps to %s" % figure_path

    
    
def plot_aucs(results_dir, figure_path, num_repeats=10):
    """ Plot cross-validated AUCs, averaged over multiple repeats.

    Parameters
    ----------
    results_dir: path
        Path to the directory containing the results.

    figure_path: path
        Path to the file where to save the plot.

    num_repeats: (int, None)
        Number of repeated experiments.        
    """
    # method:[list of repeated AUCs]
    aucs_dict = {'nodes_l1logreg': [],
                 'lioness_l1logreg': [],
                 'regline_l1logreg': []}#, 'sfan_l2logreg': []

    # read AUCs
    for repeat_idx in range(num_repeats):
        repeat_dir = '%s/repeat%d/results' % (results_dir, repeat_idx)

        # L1-logreg on node weights
        with open('%s/results.txt' % repeat_dir, 'r') as f:
            for line in f:
                ls = line.split()
                if ls[0] == "AUC:":
                    auc = float(ls[1])
                    break
            f.close()
        aucs_dict['nodes_l1logreg'].append(auc)

        # L1-logreg on edge weights
        for ntwk in ['lioness', 'regline']:
            with open('%s/%s/results.txt' % (repeat_dir, ntwk), 'r') as f:
                for line in f:
                    ls = line.split()
                    if ls[0] == "AUC:":
                        auc = float(ls[1])
                        break
                f.close()
            aucs_dict['%s_l1logreg' % ntwk].append(auc)

        # # L2-logreg on sfan-selected features
        # with open('%s/sfan/results.txt' % repeat_dir, 'r') as f:
        #     for line in f:
        #         ls = line.split()
        #         if ls[0] == "AUC:":
        #             auc = float(ls[1])
        #             break
        #     f.close()
        # aucs_dict['sfan_l2logreg'].append(auc)


    # Plot AUCs
    # auc_data has num_repeats rows, as many columns as methods
    auc_data = np.array([aucs_dict['nodes_l1logreg'],
                         aucs_dict['lioness_l1logreg'],
                         aucs_dict['regline_l1logreg']])
    auc_data = np.transpose(auc_data)

    plt.figure(figsize=(10, 5))

    bp = plt.boxplot(auc_data)
    plt.setp(bp['boxes'], color=blue_color)
    plt.setp(bp['whiskers'], color=blue_color)
    plt.setp(bp['fliers'], color=blue_color)
    plt.setp(bp['medians'], color=orange_color)

    plt.plot(range(5), [0.74 for x in range(5)], 
             ls='--', color='#666666')
    plt.text(0.75, 0.745, 'FERAL (paper)', color='#666666')

    plt.title('Cross-validated L1-logistic regression over %d repeats' % num_repeats)
    plt.ylabel('AUC')
    labels = ('Nodes', 'Edges (Lioness)', 'Edges (Regline)')
    plt.xticks(range(1, 4), labels)#, rotation=35)
    plt.ylim(0.63, 0.78)

    plt.savefig(figure_path, bbox_inches='tight')
    print "Saved AUCs to %s" % figure_path

    
def main():
    """ Plot results from cross-validation experiments.

    Example:
        $ python create_plots.py ../outputs/U133A_combat_RFS -r 10
    """
    parser = argparse.ArgumentParser(description="Plot results from CV experiments",
                                     add_help=True)
    parser.add_argument("results_dir", help="Path to results")
    parser.add_argument("-r", "--num_repeats", help="Number of repeats",
                        type=int, default=10)
    args = parser.parse_args()

    ### Subtype-stratified cross-validation ###
    results_dir = '%s/subtype_stratified' % args.results_dir

    # AUC
    figure_path = '%s/subtype_stratified_auc.pdf' % results_dir
    plot_aucs(results_dir, figure_path, num_repeats=args.num_repeats)

    # Number of features
    figure_path = '%s/subtype_stratified_numf.pdf' % results_dir
    plot_numf(results_dir, figure_path, num_repeats=args.num_repeats)
    
    # Consistency index
    figure_path = '%s/subtype_stratified_cix.pdf' % results_dir
    plot_cixs(results_dir, figure_path, num_repeats=args.num_repeats)
    
    # Fisher overlap
    figure_path = '%s/subtype_stratified_fov.pdf' % results_dir
    plot_fovs(results_dir, figure_path, num_repeats=args.num_repeats)
    

if __name__ == "__main__":
    main()

