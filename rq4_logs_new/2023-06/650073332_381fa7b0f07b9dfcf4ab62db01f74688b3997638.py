import argparse
from PIL import Image
from typing import Union

DESCRIPTION = \
"Channel Packer Utility.\n\
Soren Saville Scott. 2023. MIT.\n\n\
Packs provided channels into an RGBA image.\n\
Either a path to a texture or an integer in the range 0-255 may be provided to each switch.\
Appending a ':' followed by any of 'R,G,B,A' to a path string will pull that channel out of the desired image."


def make_image(input: 'Union[str, int, None]', size: 'Union[tuple[int,int], None]'=None) -> 'Image':
    if type(input) is int:
        if size is None:
            return Image.new("L", (8,8), input)
        else:
            return Image.new("L", size, input)

    elif type(input) is str:
        if (input.endswith(":R")):
            return Image.open(input.replace(":R", "")).getchannel('R')
        elif (input.endswith(":G")):
            return Image.open(input.replace(":G", "")).getchannel('G')
        elif (input.endswith(":B")):
            return Image.open(input.replace(":B", "")).getchannel('B')
        elif (input.endswith(":A")):
            return Image.open(input.replace(":A", "")).getchannel('A')
        else:
            return Image.open(input).convert("L") 



parser = argparse.ArgumentParser(prog="pack.py", 
                                 description=DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument("-r", "--red", default=0, metavar="r.png or 0-255", help="Red Channel Input. Default value is 0")
parser.add_argument("-g", "--green", default=255, metavar="g.png or 0-255", help="Green Channel Input. Default value is 255")
parser.add_argument("-b", "--blue", default=255, metavar="b.png or 0-255", help="Blue Channel Input. Default value is 255")
parser.add_argument("-a", "--alpha", default=0, metavar="a.png or 0-255", help="Alpha Channel Input. Default value is 0")
parser.add_argument("output", help="The output filename")


args = parser.parse_args()

#work out whether the input args are uniforms or textures 
bands = []
for arg in (args.red, args.green, args.blue, args.alpha):
    try:
        value = int(arg)
        bands.append(value)
    except ValueError:
        if arg is None:
            bands.append(None)
        else:
            bands.append(arg)
         

#early exit, all bands are uniform. Write 8x8 image with the values
if all(type(x) is int for x in bands):
    red = make_image(bands[0])
    green = make_image(bands[1])
    blue = make_image(bands[2])
    alpha = make_image(bands[3])

    Image.merge("RGBA", (red, green, blue, alpha)).save(args.output)
    quit()


#find first non-uniform band input, use it as the size reference. 
#Assumes all your inputs have the same size!
index, path = next(x for x in enumerate(bands) if type(x[1]) is str)


images = {"r": None, "g": None, "b": None, "a": None}
if index == 0:
    red = make_image(path)
    size = (red.width, red.height)
    images['r'] = red
elif index == 1:
    green = make_image(path)
    size = (green.width, green.height)
    images['g'] = green
elif index == 2:
    blue = make_image(path)
    size = (blue.width, blue.height)
    images['b'] = blue
elif index == 3:
    alpha = make_image(path)
    size = (alpha.width, alpha.height)
    images ['a'] = alpha



for key, band in zip(("r", "g", "b", "a"), bands):
    if type(band) is int:
        images[key] = make_image(band, size)
    elif images[key] is None:
        images[key] = make_image(band)

print(images)
Image.merge("RGBA", tuple(images.values())).save(args.output)