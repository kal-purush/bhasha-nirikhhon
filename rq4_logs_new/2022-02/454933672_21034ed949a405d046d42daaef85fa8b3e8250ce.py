import gzip
import json
import numpy as np
import random
import os
from utils.map_tools import get_maps
import spacy
from utils.map_tools import execution_time
nlp = spacy.load("en_core_web_sm")

class InferenceModel(object):
    def __init__(self, split="train") -> None:
        super().__init__()

        data_path = f"data/annt/{split}/{split}"
        self.map_root = f"data/maps/gmap_floor1_mpp_0.05_channel_last_with_bounds"

        with gzip.open(data_path+".json.gz", "rt") as f:
            annt_json = json.load(f)
        
        with gzip.open(data_path+"_gt.json.gz", "rt") as f:
            self.gt_json = json.load(f)
        
        episodes = annt_json['episodes']
        if os.environ.get('DEBUG', False): 
            self.eps = random.sample(episodes, 10)
        else:
            self.eps = episodes

    def inference(self, ep_id):
        pass

    def eval(self, ep_id):
        pass
    
    @execution_time
    def run(self):
        counter = dict()
        for episode in self.eps:
            ep_id = episode["episode_id"]
            instruction = episode['instruction']['instruction_text']
            scene_name = episode['scene_id'].split('/')[1]
            nav_map, room_map, obj_maps, grid_dimensions, bounds = get_maps(scene_name, self.map_root)
            doc = nlp(instruction)
            print("="*10,f"Episode: {ep_id}","="*10)
            print(instruction)
            for chunk in doc.noun_chunks:
                if "dinning room" in chunk.text or "living room" in chunk.text:
                    print(chunk.text.replace('the', "").replace("a", "").strip())
                else:
                    #print(chunk.text, chunk.root.text)
                    print(chunk.root.text)

if __name__ == "__main__":
    model = InferenceModel(split="train")
    model.run()