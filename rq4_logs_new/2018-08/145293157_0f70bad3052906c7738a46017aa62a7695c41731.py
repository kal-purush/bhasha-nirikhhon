from argparse import ArgumentParser
from Experiment import PredictionExperiment


if __name__ == '__main__':
    # command line arguments
    parser = ArgumentParser(description='Required arguments')
    parser.add_argument('-a','--algorithm', help='Prediction Algorithm', required=False)
    parser.add_argument('-r', '--revision', help='Source code revision', required=False)
    parser.add_argument('-d', '--debug', action='store_true')
    args = parser.parse_args()

    instance = PredictionExperiment(algorithm=args.algorithm)

    experiment_id = 1
    instance.batchId = experiment_id

    instance.runMainExperiment()