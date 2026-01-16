from PIL import Image
import glob
import os

outdir = '.'
indir = '.'
for filename in os.listdir(indir):
    if filename.endswith('.bmp') or filename.endswith('.BMP'):
        print('Converting %s to PNG.'%(filename))
        Image.open(filename).save(os.path.join(outdir, '%s.png'%(filename)), format='png')