import sys
from PIL import Image
import json
import os

if len(sys.argv) < 2:
    print("Please pass the config file")
    sys.exit(1)

if not os.path.isfile(sys.argv[1]):
    print("Not a valid file")
    sys.exit(1)

config = ''
with open(sys.argv[1]) as f:
    config = f.read()
try:
    config = json.loads(config)
except JSONDecodeError:
    print("Error")

if 'images' not in config:
    print("Error: Must pass image names")
    sys.exit(1)
imgs = config['images']
if not isinstance(imgs,list):
    print("Error: images must be a list")
    sys.exit(1)
if len(imgs) != 7:
    print("Error: images must have exactly 7 arguments")
    sys.exit(1)

if 'ouputSize' not in config:
    config['outputSize'] = (512,512)
    
size = config['outputSize']
for index,img in enumerate(imgs):
    if 'name' not in img:
        print("Error: name must be supplied")
        sys.exit(1)
    if 'size' not in img:
        if index == 0:
            img['size'] = (size[0]//2,size[1]//2)
        else:
            img['size'] = (size[0]//4,size[1]//4)
    else:
        img['size'] = tuple(map(int,img['size'].split(',')))
    
    if 'position' not in img:
        if index == 0:
            img['position'] = (0,0)
        if index == 1:
            img['position'] = (size[0]//2,0)
        if index == 2:
            img['position'] = (size[0]*3//4,size[1]//4)
        if index == 3:
            img['position'] = (0,size[1]//2)
        if index == 4:
            img['position'] = (size[0]//4,size[1]*3//4)
        if index == 5:
            img['position'] = (size[0]//2,size[1]//2)
        if index == 6:
            img['position'] = (size[0]*3//4,size[1]*3//4)
    else:
        img['position'] = tuple(map(int, img['position'].split(',')))
    if 'rotateAngle' not in img:
        img['rotateAngle'] = -90 if index == 6 else 0

if 'background' not in config:
    config['background'] = (255,255,255)
else:
    config['background'] = tuple(map(int,config['background'].split(',')))

if 'outputName' not in config:
    config['outputName'] = 'loss.png'

if 'outputMode' not in config:
    config['outputMode'] = 'RGB'

print(config)
    
output = Image.new(config['outputMode'],size,config['background'])
for img in imgs:
    output.paste(Image.open(img['name']).resize(img['size']).rotate(img['rotateAngle']),img['position'])

output.save(config['outputName'])