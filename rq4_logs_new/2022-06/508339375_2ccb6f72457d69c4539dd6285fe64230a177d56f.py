import os
import json

cfg_file="../menu_items.json"

def read_cfg():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    cfg_file_path = os.path.join(cur_dir, cfg_file)
    with open(cfg_file_path, 'r') as f:
        file_str = f.read()
    json_obj = json.loads(file_str)
    return json_obj
