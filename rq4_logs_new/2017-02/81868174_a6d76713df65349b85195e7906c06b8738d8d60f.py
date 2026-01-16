from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import matplotlib.cm as cm
import PIL.Image as Image

def create_jpeg(img, output_filename):
    jet = cm.get_cmap('jet')
    mn = np.min(img)
    mx = np.max(img)
    img = img - mn
    img /= (mx-mn)
    img = np.maximum(0.0, np.minimum(1.0, img))
    img = jet(img)[:,:,0:3]
    img *= 255.0
    img = np.uint8(img)
    img = Image.fromarray(img)
    img.save(output_filename)
    print("saved %s" % output_filename)

def is_closed_five_point_box(points):
    assert len(points)==5
    ptA = points[0]
    # need to finish
    return True