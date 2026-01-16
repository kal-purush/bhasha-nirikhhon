import argparse
import os
import json

argp = argparse.ArgumentParser()
argp.add_argument('--input_json_files_dir',
    help="Path to the directory containing json files (stored as array of json objects)", required=True)
argp.add_argument('--output_jsonline_files_dir',
    help="Path to the directory to output jsonline files", required=True)
args = vars(argp.parse_args())


if __name__ == "__main__":
    for dataset in ["dev", "indiana_dev", "train"]:
        print("Converting for dataset {}".format(dataset))
        tok_json_file = open(os.path.join(args["input_json_files_dir"], f"{dataset}_tok.json"), "r")
        json_data = json.load(tok_json_file)

        with open(os.path.join(args["output_jsonline_files_dir"], f"{dataset}_tok_lines.json"), "w") as outfile:
            for data in json_data:
                data_to_write = {}
                data_to_write["findings"] = data["findings"].split()
                data_to_write["impression"] = data["impression"].split()
                json.dump(data_to_write, outfile)
                outfile.write("\n")
    print("Finished converting.")