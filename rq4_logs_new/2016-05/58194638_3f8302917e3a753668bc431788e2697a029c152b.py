
# coding: utf-8

# # Assignment 4 and 5
# 
# ## imports

# In[ ]:

from h5py import File

from imagenet import ImagenetModel

import json

from scipy.misc import imshow

from numpy import array, where, take

from random import randint

from theano.tensor import tensor4
from theano import function

get_ipython().magic('matplotlib inline')


# ## constants

# In[ ]:

IMAGE_DATA_H5PY_FILE = './data/cocotalk.h5'
IMG_DIR = './data/'
JSON_COCOTALK = './json/cocotalk.json'
JSON_COCO_RAW = './json/coco_raw.json'
MATLAB_MODEL_FILE = './model/imagenet-vgg-verydeep-16.mat'

WORD_PROBABILITY_TH = 0.01
R = .299
G = .587
B = .144


# ## load and build model

# In[ ]:

imdl = ImagenetModel(MATLAB_MODEL_FILE)

x = tensor4('x', dtype='float32')
x_ = x
for layer in imdl.layers:
    x_ = layer.apply(x_)
    
the_func = function(inputs=[x], outputs=[x_], allow_input_downcast=True)


# ## sample
# ### load

# In[ ]:

images_file = File(IMAGE_DATA_H5PY_FILE, 'r')
images = images_file['/images/']
label_length = images_file['/label_length/']


# In[ ]:

with open(JSON_COCOTALK) as f:
    js = json.loads(f.read())
    vocab = js['ix_to_word']
with open(JSON_COCO_RAW) as f:
    meta = json.loads(f.read())
    captions = {}
    pos2id_map = {}
    i = 0
    for e in js['images']:
        pos2id_map[e['id']] = i
        i+= 1
    for e in meta:
        captions[pos2id_map[e['id']]] = e['captions']
        


# ### inspect the data or network

# In[ ]:




# ### compute labels

# In[ ]:

def get_label(word_probs, entry_id, sep=' '):
    p_vals = word_probs[0].flatten()
    p_vals.sort()
    ids = where(p_vals >= WORD_PROBABILITY_TH)
    label_candidates = []
    for id in ids[0]:
        label_candidates.append(vocab[str(id)])
    label_candidates.reverse()    
    return sep.join(label_candidates)


# In[ ]:

entry_id = randint(0, images.shape[0]-1)
result = the_func([images[entry_id]])


# In[ ]:

label = get_label(result, image_id)

print(label, '\ntrue labels:', captions[entry_id])
imshow(images[entry_id])


# ### close

# In[ ]:

images_file.close()


# In[ ]:


