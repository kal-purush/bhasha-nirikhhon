#! /usr/bin/env python

import math
import csv
import os
import sys
import argparse
import collections
import re
from dendropy.calculate import statistics
from spectrasophy import utility

class SpectrasophySummarizer(object):

    def __init__(self,
            field_delimiter="\t",
            exclude_field_patterns=None,
            include_only_field_patterns=None,
            ):
        self.field_delimiter = field_delimiter
        self.all_fieldnames = None
        self.other_fieldnames = None
        self.stat_fieldnames = None
        self.stat_fieldnames_check = None
        self.other_fieldname_check = None
        self.stat_values = []
        self.other_values = []

    def summarize(self, target_data_filepath,):
        with utility.universal_open(target_data_filepath) as src:
            reader = csv.DictReader(
                    src,
                    delimiter=self.field_delimiter,
                    quoting=csv.QUOTE_NONE)
            categorical_params = collections.OrderedDict()
            continuous_params = collections.OrderedDict()
            for row_idx, row in enumerate(reader):
                for key_idx, key in enumerate(reader.fieldnames):
                    if key in categorical_params:
                        categorical_params[key][row[key]] += 1
                    elif key in continuous_params:
                        continuous_params[key].append(float(row[key]))
                    else:
                        if key in ("param.DivTimeModel", "param.numDivTimes"):
                            val = row[key]
                            is_categorical = True
                        else:
                            try:
                                val = float(row[key])
                                is_categorical = False
                            except ValueError:
                                val = row[key]
                                is_categorical = True
                        if is_categorical:
                            categorical_params[key] = collections.Counter()
                            categorical_params[key][val] += 1
                        else:
                            continuous_params[key] = [val]
            output_prefix = os.path.splitext(os.path.basename(target_data_filepath))[0]
            with utility.universal_open(output_prefix + ".summary.continuous.tsv", "w") as dest:
                row_results = collections.OrderedDict()
                for param_idx, param_name in enumerate(continuous_params):
                    values = continuous_params[param_name]
                    row_results["param"] = param_name
                    summary = statistics.summarize(values)
                    row_results["mean"] = summary["mean"]
                    row_results["var"] = summary["var"]
                    row_results["sd"] = summary["sd"]
                    row_results["min"] = summary["range"][0]
                    row_results["max"] = summary["range"][1]
                    row_results["hpd5"] = summary["hpd95"][0]
                    row_results["hpd95"] = summary["hpd95"][1]
                    try:
                        row_results["quant5"] = summary["quant_5_95"][0]
                        row_results["quant95"] = summary["quant_5_95"][1]
                    except TypeError:
                        row_results["quant5"] = "NA"
                        row_results["quant95"] = "NA"
                    if param_idx == 0:
                        dest.write(self.field_delimiter.join(row_results.keys()) + "\n")
                    dest.write(self.field_delimiter.join("{}".format(v) for v in row_results.values()) + "\n")
            for param_idx, param_name in enumerate(categorical_params):
                with utility.universal_open(output_prefix + ".summary.{:02d}.{}.tsv".format(param_idx, param_name), "w") as dest:
                    param_counter = categorical_params[param_name]
                    total = float(sum(param_counter.values()))
                    for category_idx, (category_name, category_count) in enumerate(param_counter.most_common()):
                        row_results = collections.OrderedDict()
                        row_results["label"] = category_name
                        row_results["freq"] = category_count/total
                        row_results["count"] = category_count
                        if category_idx == 0:
                            dest.write(self.field_delimiter.join(row_results.keys()) + "\n")
                    dest.write(self.field_delimiter.join("{}".format(v) for v in row_results.values()) + "\n")

def main():
    parser = argparse.ArgumentParser(
            description="SPECTRASOPHY Rejection Sampler",
            )
    parser.add_argument(
            "posteriors_filepath",
            help="Path to posteriors parameter file.")
    processing_options = parser.add_argument_group("Processing Options")
    processing_options.add_argument("--field-delimiter",
        type=str,
        default="\t",
        help="Field delimiter (default: <TAB>).")
    run_options = parser.add_argument_group("Run Options")
    run_options.add_argument(
            "-q", "--quiet",
            action="store_true",
            help="Work silently.")
    args = parser.parse_args()
    summarizer = SpectrasophySummarizer(field_delimiter=args.field_delimiter)
    summarizer.summarize(args.posteriors_filepath)

if __name__ == "__main__":
    main()



