import os
import json

def collate_json_files(input_folder, prefix, output_file):
    json_array = []
    for filename in os.listdir(input_folder):
        if filename.startswith(prefix) and filename.endswith('.json'):
            file_path = os.path.join(input_folder, filename)
            with open(file_path, 'r') as f:
                json_object = json.load(f)
                json_array.append(json_object)
    
    with open(os.path.join(input_folder, output_file), 'w') as out_file:
        json.dump(json_array, out_file, indent=4)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(script_dir, "..", "..", "contributors-and-contributions")
    collate_json_files(json_dir, 'contributors_', 'contributors.json')
    collate_json_files(json_dir, 'contributions_', 'contributions.json')

if __name__ == "__main__":
    main()