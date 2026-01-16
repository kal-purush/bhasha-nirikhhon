from features.feature import IdentityWindowFeature, TrainableFeature
from features.simple_filter import *
from features.peak_feature import *
from features.envelope import *
from features.breath_cnn_feature import *
import argparse
import json
import h5py
import os

feature_dict = {
        "id" : IdentityWindowFeature,
        "ea" : WindowEnvelopesAmplitude,
        "ptp" : WindowPeakTroughPeriods,
        "butter" : SimpleButterFilter,
        "spline" : SimpleSplineFilter,
        "local_norm" : SimpleLocalNorm,
        "breath_cnn" : BreathCNNFeature
        }

class Model:
    def __init__(self):
        self._features_list = []
        self._features_dict = {}

    def add_feature(self, feature_name, feature):
        if feature_name in self._features_dict.keys():
            raise ValueError("Duplicate name {} found in model description. Names must be unique.".format(feature_name))
        self._features_list.append(feature)
        self._features_dict[feature_name] = feature

    def get(self, identifier):
        if type(identifier) is str:
            return self._features_dict.get(name)
        else:
            return self._features_list[identifier]

    def get_list(self):
        return self._features_list

    def get_dict(self):
        return self._features_dict

    def __call__(self, signal):
        self._features_list[-1].calc_features(signal)

def build_model(sampling_rate,model_json_path):
    """ Given a json descriptor, generates the model as a dictionary of features. """
    model_spec = json.load(open(model_json_path, 'r'))

    model = Model()
    for parameter_dict in model_spec:
        feature_type = parameter_dict["type"]
        constructor = feature_dict[feature_type]
        in_feature_ids = parameter_dict["in_features"]
        in_features = [model.get(x) for x in in_feature_ids]
        feature = constructor(sampling_rate,in_features,parameter_dict["params"])

        feature_name = parameter_dict.get("name")
        if feature_name is None:
            # No name given, generate unique name by trial and error
            i = 0
            while True:
                feature_name = "{}{}".format(feature_type, i)
                if feature_name not in model.get_dict().keys():
                    break
                i+=1
        model.add_feature(feature_name, feature)

    return model
