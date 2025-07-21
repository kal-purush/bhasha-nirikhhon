import glob
from pathlib import Path
import os
import json
from collections import defaultdict
file_ext_dict ={}
target_folder = 'rq4_logs'
sub_folders = glob.glob(f"{target_folder}/*")
year_months = set()
file_ext_dict={}
# for ext in ['.py', '.java', '.js', '.ts', '.cs']:
for folder in sub_folders:
    files = glob.glob(f"{folder}/*")
    # print(folder, len(files))
    for file_ in files:
        ext = Path(file_).suffix.lower()
        
        folder_name = os.path.basename(folder)
        parts = folder_name.split('-')
        if len(parts) >= 2:
            year, month = parts[0], parts[1]
        else:
            year, month = "unknown", "unknown"

        base_name = os.path.basename(file_)
        json_name = f"{os.path.splitext(base_name)[0]}_{year}_{month}.json"
        out_dir = f"code_paerser_data_rq4/{year}-{month}"
        out_path = os.path.join(out_dir, json_name)
        file_ext_dict[out_path] = ext
        # break  

# file_ext_dict

target_folder = 'code_paerser_data_rq4'
sub_folders = glob.glob(f"{target_folder}/*")
year_months = set()
file_ext_dict_count={}
# for ext in ['.py', '.java', '.js', '.ts', '.cs']:
for folder in sub_folders:
    file_ext_dict_count=defaultdict(int)
    files = glob.glob(f"{folder}/*")
    for file_ in files:
        ext = file_ext_dict[file_]
        file_ext_dict_count[ext]+=1
    print(folder, file_ext_dict_count)
