# script to use DataSelector class

import argparse
# import os
# import sys
# sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from data_selector.data_selector import DataSelector

if __name__ == "__main__":

    # define input parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folders', help='Path to YAML file describing data folders.', required=True)
    parser.add_argument('--dataset_config', help='Path to YAML file how the new datasets should be created.', required=True)
    parser.add_argument('--output_train', help='Path to generated train dataset.', required=True)
    parser.add_argument('--output_val', help='Path to generated val dataset.', required=True)
    args = parser.parse_args()

    print('Generating datasets...')

    ds = DataSelector(data_folders_filepath=args.data_folders, dataset_config_filepath=args.dataset_config)
    ds.generate_dataset(train_dataset_folder=args.output_train, val_dataset_folder=args.output_val)

    print('Generating datasets DONE.')