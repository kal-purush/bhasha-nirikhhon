#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from PIL import Image, ImageDraw, ImageFont, ImageOps
from img2angle import all_angles
import time
import os
from tqdm import tqdm


type = "IcoMoon"

fontfile = ""
prefix = "z_"
letters = ""
g_width = 10
g_height = 10
dir = ""
out_dir = ""

if type == "IcoMoon":
    fontfile = "" #IcoMoon-Free.ttf"
    prefix = "zi_"
    dir = "IcoMoon-Free-master/PNG/64px/"
elif type == "helvetica_num_4x4":
    fontfile = "helveticaneuelight.ttf"
    prefix = "z_4x4_"
    letters = "0123456789"
    g_width = 4
    g_height = 4
elif type == "helvetica":
    fontfile = "helveticaneuelight.ttf"
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890"
if dir == "":
    dir = "buchstabe_%s" % prefix
if out_dir == "":
    out_dir = "tmp_buchstabe_%s" % prefix
try:
    os.mkdir(dir)
except:
    pass
try:
    os.mkdir(out_dir)
except:
    pass

def createImg(symbol, name, fontfile):
    filename = '%s/%s.png' % (dir, name)
    if os.path.exists(filename):
        return
    image = Image.new("L", (200,200), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.truetype(fontfile, 150)
    draw.text((0, 0), symbol, 0, font=font)
    time.sleep(0.01);
    #print(imageBox)
    #image.save(filename)
    imageBox = ImageOps.invert(image).getbbox()
    cropped=image.crop(imageBox)
    cropped.save(filename)


def createCanoeLetters(letter, f):
    angles, _ = all_angles("%s/%s.png"%(dir, letter), out_dir, invert=True, edge=150, filter=14, alphacolor=255, size=(g_width, g_height))
    angles2, _ = all_angles("%s/%s.png"%(dir,letter.upper()), out_dir, invert=True, edge=150, filter=14, alphacolor=255, size=(g_width, g_height))

    f.write("int %s%s(int row, int col, int upper) {\n" % (prefix, letter))
    f.write("  int z[%d][%d] = %s;\n" % (g_width, g_height, str(angles).replace("[","{").replace("]","}")))
    f.write("  int z2[%d][%d] = %s;\n" % (g_width, g_height, str(angles2).replace("[","{").replace("]","}")))
    f.write("  if (!upper) return z[row][col]; else return z2[row][col];\n")
    f.write("}\n")
    f.write("int %s%s(int row, int col) { return %s%s(row,col,0); }\n" % (prefix, letter, prefix, letter))
    f.write("\n")

def createCanoeNumbers(f):
    f.write("int %s_%d%d_numbers(int row, int col, int num) {\n" % (prefix, g_height, g_width))
    for letter in "0123456789":
        angles, _ = all_angles("%s/%s.png"%(dir,letter), out_dir, invert=True, edge=150, filter=14, alphacolor=255, size=(g_width, g_height))
        # for i in range(9):
        #     if i+1 >= len(angles):
        #         angles.append([900]*10)
        #     for j in range(9):
        #         if j+1 >= len(angles[i]):
        #             angles[i].append(900)
        print(angles)
        print(len(angles))
        print(len(angles[0]))
        f.write("  int z%s[%d][%d] = %s;\n" % (letter, g_height, g_width, str(angles).replace("[","{").replace("]","}")))
    f.write("  switch(num) {\n")
    for letter in "0123456789":
        f.write("    case %s: return z%s[row][col];\n" % (letter, letter))
    f.write("  }\n")
    f.write("}\n")


def getCName(prefix, name):
    return prefix + name.replace("-", "_")

count = 0
def createCanoeSymbols(name):
    global count
    count += 1
    angles, _ = all_angles("%s/%s.png"%(dir,name), out_dir, invert=True, edge=150, filter=50, alphacolor=255, progress=False, size=(g_width, g_height))
    return name, angles

if fontfile != "":
    for letter in letters:
        createImg(letter, letter, fontfile)
    with open("a.can", "a") as f:
        for letter in letters:
            createCanoeLetters(letter, f)
            # createCanoeNumbers(f)
else:
    # -4 removes file ending
    chars = [x[:-4] for x in os.listdir(dir)]
    import multiprocessing

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    angles = {}
    with tqdm(total=len(chars), desc="Main-loop") as pbar:
        #angles = pool.map(createCanoeSymbols, [x[1] for x in chars])
        for i, val in enumerate(pool.imap_unordered(createCanoeSymbols, [x for x in chars])):
            name, angle = val
            pbar.update(1)
            angles[name] = angle

    with open("a.can", "w") as f:
        i = 0
        f.write("includes\n")
        f.write("{\n")
        f.write("}\n")
        f.write("variables\n")
        f.write("{\n")
        f.write("int %sz[%d][%d][%d] = {\n" % (prefix, len(chars), g_width, g_height))
        for name in chars:
            angle = angles[name]
            comma = ","
            if i == len(chars):
                comma = ""
            f.write("  %s%s\n" % (str(angle).replace("[","{").replace("]","}"), comma))
            i += 1
        f.write("}\n\n")
        i = 0
        for name in chars:
            f.write("int %s(int row, int col) {" % getCName(prefix, name))
            f.write("return %sz[%d][row][col];" % (prefix, i))
            f.write("}\n")
            i += 1
        f.write("\n")
    
        f.write("int %sbyid(int row, int col, int id) {\n" % (prefix))
        f.write("  return %sz[id][row][col];\n" % (prefix))
        f.write("}\n")
        f.write("\n")
        

        f.write("int %sbyid(int id, char s[]) {\n" % (prefix))
        f.write("  switch(id){\n")
        i = 0
        for name in chars:
            f.write("    case %d: snprintf(s, elcount(s), \"%s\"); return;\n" % (i, name))
            i+=1
        f.write("  }\n")
        f.write("}\n")