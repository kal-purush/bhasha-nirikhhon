from utils.model import feature_dict
import argparse

parser = argparse.ArgumentParser(description='Prints the param description of a given .')
parser.add_argument('feature_name', type=str, help='Name of the feature as used in the feature_dict in utils/model.py.')
args = parser.parse_args()

print(args.feature_name)
feature = feature_dict.get(args.feature_name)
if feature is None:
    print("Specified feature was not found in the feature_dict in utils/model.py")
else:
    print(feature.get_param_template())