import pandas as pd
import h5py
from keras.models import load_model
import sys
sys.path.append("/mnt/disks/disk0/deep_moon_working_dir/DeepCrater/utils")
sys.path.append("/mnt/disks/disk0/deep_moon_working_dir/DeepCrater")
import score_utils as sc 
import utils.processing as proc
model = load_model("/mnt/disks/disk0/deep_moon_working_dir/data/Silburt/model_keras2.h5")

data_path = '/mnt/disks/disk0/deep_moon_working_dir/data/my_test_data'
images = h5py.File(data_path + '/train_images.hdf5', 'r')
sample_data = {'imgs': [images['input_images'][...].astype('float32'),
                        images['target_masks'][...].astype('float32')]}
proc.preprocess(sample_data)
sd_input_images = sample_data['imgs'][0]
sd_target_masks = sample_data['imgs'][1]
ctrs = pd.HDFStore(data_path + '/train_craters.hdf5', 'r')

def get_craters_from_database(ctrs, image_id):
    my_craters = ctrs["/img_{:05d}".format(image_id)]
    labeled_craters = []
    for index, row in my_craters.iterrows():
        labeled_craters.append([int(round(row['x'])), int(round(row['y'])), int(round(row['Diameter (pix)'])/2)])
    return labeled_craters
    
from timeit import default_timer as timer
import math
import numpy as np
import template_match_target as tmt



total_images_count = 300
match_count = 0
fn_count = 0
fp_count = 0
start = timer()
#selected_images_indices = (np.random.rand(sample_size)*total_images_count).astype(int)
pred = model.predict(sd_input_images)
for i in range(total_images_count):
    labeled = get_craters_from_database(ctrs, i)   
    predicted = tmt.template_match_t(pred[i].copy(), minrad=2.)
    [match, fn, fp] = sc.match_circle_lists(labeled, predicted,0.1)
    print("image id = {}, correct = {},  fn = {}, fp ={}".format(i, len(match), len(fn), len(fp))) 
    match_count += len(match)
    fn_count += len(fn)
    fp_count += len(fp)
print("correct = {},  fn = {}, fp ={}".format(match_count, fn_count, fp_count))    
end = timer()
print("elapsed time {}".format(end - start))
