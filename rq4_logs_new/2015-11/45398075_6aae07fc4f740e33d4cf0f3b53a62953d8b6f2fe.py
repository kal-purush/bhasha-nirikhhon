#
# Tests the JPEG-size attack on raw TIFF images, resized to
# 405 x 270 (a size similar to the one of the Capy challenges)
#
# This experiment is to check a interesting question from Prof. Kuhn during my
# presentation mentioning the attack published to Capy, KeyCAPTCHA, Garb and any other
# puzzle (image recomposition) CAPTCHAs
#

# Instructions to repeat the experiment:
# 1 - download RAISE dataset of RAW images from http://mmlab.science.unitn.it/RAISE/
# 2 - convert them to .PNG (lossless compression) from RAW TIFF, as RAW TIFF is poorly supported by Python PIL library
#     we use the following command line (installing first imagemagick)
#        $mogrify -format png *.TIF
#     and then delete the thumbnails produced
#        $rm *-1.png
# 3 - run the experiment (function "main"). There is an option that affects the result: 
#     if the images are resized & cropped or just cropped to a size similar to Capy
#     - if they are resized & cropped, we get aprox. a 30% success ratio
#     - if they are only cropped, we get aprox. a 25.7% success ratio
#     So, the performance of the attack seems possibly related to the amount of "visual information" in the image

# the experiment - what this program does:
# 1 - pick each image in the directory (RAW TIFF image of 405 x 270 pixels)
# 2 - substitute a 80 x 90 pixel square with a subimage from another random image in the directory
#     this square can be located anywhere in a 10 x 10 pixel grid (as does Capy)
# 3 - perform the JPEG-size attack (described better in the paper 
#     "Using JPEG to Measure Image Continuity and Break Capy and Other Puzzle CAPTCHAs", Internet Computing Magazine, Nov/Dec 2015), 
#     with quality = 100, and check whether it is successfull
# 4 - show statistics (& save them)

from PIL import Image, ImageDraw, ImageOps
import PIL
import os
import random

# some global config variables
img_size = [405, 270]
puzzle_size = [80, 90]
img_file_extension = 'png'

# runs the experiment, inputs are:
#
# - image_dir : where the RAW images are located
# - temp_img_dir : where to create and delete temporary JPEG images
# - save_challenges : if you want to save the resulting answers or not
# - dir_challenges : if the previous is True, then where to save the answers
# - verbose : more or less informative output

def main(imgage_dir, temp_img_dir = '/tmp' , save_challenges = True, dir_challenges = '/var/capy-experiment', resize_image = True, verbose = True):
    global img_file_extension
    imgs = [o for o in os.listdir(imgage_dir) if not(os.path.isdir(os.path.join(imgage_dir,o))) and o.split('.')[1] == img_file_extension]
    solved_experiments = []
    num_exp = 0
    for img in imgs:
        if verbose:
            print 'solving challenge with ' + img
        if num_exp % 50 == 0:
            num_correct = sum(map(lambda x:x[0], solved_experiments))
            if len(solved_experiments) > 0:
                print str(num_correct) + ' challenges solved (' + str(num_correct * 100.0 / len(solved_experiments)) + '%)'
            if save_challenges:
                results_fn = os.path.join(dir_challenges, 'log.txt')
                print 'detailed results in ' + results_fn
                f_results = open(results_fn, 'w')
                f_results.write('\n'.join(map(lambda x:str(x), solved_experiments)))
                f_results.close()                
        pil_image_background, pil_img_puzzle, true_position = create_test(imgage_dir, img, imgs, resize_image)
        if save_challenges:
            full_dir = os.path.join(dir_challenges, 'challenges')
            if not(os.path.exists(full_dir)):
                os.makedirs(full_dir)
            pil_image_background.save(os.path.join(full_dir, img.replace('.'+img_file_extension, '-challenge.' + img_file_extension)))#, 'JPEG', quality = 100)  
            pil_img_puzzle.save(os.path.join(full_dir, img.replace('.'+img_file_extension, '-puzzle-piece.' + img_file_extension)))
        minh_position, minh_size, mins_pil_img_sol = getMinJPEGfileSize(temp_img_dir, pil_image_background, pil_img_puzzle)
        if minh_position == true_position:
            solved = 1
            subdir = 'correct'
            if verbose:
                print 'correct, pos ' + str(minh_position) 
        else:
            solved = 0
            subdir = 'wrong'
            if verbose:
                print 'wrong solution! pos is ' + str(true_position) + ', but answered ' + str(minh_position)
        solved_experiments.append((solved, img, minh_position, true_position))
        if save_challenges:
            mins_pil_img_sol.save(os.path.join(full_dir, img.replace('.'+img_file_extension, '-solution.' + img_file_extension)))
        num_exp += 1
    print 'experiment ended. results:'
    num_correct = sum(map(lambda x:x[0], solved_experiments))
    print str(num_correct) + ' challenges solved (' + str(num_correct * 100.0 / len(solved_experiments)) + '%)'
    if save_challenges:
        results_fn = os.path.join(dir_challenges, 'log.txt')
        print 'detailed results in ' + results_fn
        f_results = open(results_fn, 'w')
        f_results.write('\n'.join(map(lambda x:str(x), solved_experiments)))
        f_results.close()

def create_test(imgage_dir, img, imgs, resize_image):
    global img_size 
    global puzzle_size 
    another_image = ''
    while another_image == '' or another_image == img:
        another_image = imgs[random.randint(0, len(imgs) - 1)]
    subimage_position = (random.randint(0, img_size[0] - puzzle_size[0]), random.randint(0, img_size[1] - puzzle_size[1]))
    subimage = Image.open(os.path.join(imgage_dir, another_image))
    if resize_image:
        subimage = ImageOps.fit(subimage, (img_size[0], img_size[1]), method = Image.ANTIALIAS, centering = (0.5,0.5)) 
    subimage_puzzle_piece_filling = subimage.crop((subimage_position[0], subimage_position[1], subimage_position[0] + puzzle_size[0], subimage_position[1] + puzzle_size[1]))
    challenge_background = Image.open(os.path.join(imgage_dir, img))
    # crop to img_size centered
    (width, height) = challenge_background.size
    x_start, y_start = ((width - img_size[0])/2, (height - img_size[1])/2)
    if resize_image:
        # resize full image to size, keeping aspect ratio
        centered_challenge_background = ImageOps.fit(challenge_background, (img_size[0], img_size[1]), method = Image.ANTIALIAS, centering = (0.5,0.5)) 
    else:
        # or just crop a portion from the center
        centered_challenge_background = challenge_background.crop((x_start, y_start, x_start + img_size[0], y_start + img_size[1]))
    puzzle_piece_position = (random.randint(0, img_size[0] - puzzle_size[0]) / 10, random.randint(0, img_size[1] - puzzle_size[1]) / 10)
    puzzle_piece_position = (puzzle_piece_position[0] * 10, puzzle_piece_position[1] * 10)
    puzzle_piece = centered_challenge_background.crop((puzzle_piece_position[0], puzzle_piece_position[1], puzzle_piece_position[0] + puzzle_size[0], puzzle_piece_position[1] + puzzle_size[1]))
    centered_challenge_background = mergePNG(centered_challenge_background, subimage_puzzle_piece_filling, puzzle_piece_position)
    return centered_challenge_background, puzzle_piece, puzzle_piece_position
        
def mergePNG(img_background, img_puzzle, target_position):
    new_img_background = img_background.copy()
    x1, y1 = target_position
    crop_x, crop_y = 0, 0
    if x1 < 0:
        crop_x = -x1
        x1 = 0
    if y1 < 0:
        crop_y = -y1
        y1 = 0
    new_img_puzzle = img_puzzle.copy()
    if crop_x <> 0 or crop_y <> 0:
        new_img_puzzle = new_img_puzzle.crop((crop_x, crop_y, new_img_puzzle.size[0], new_img_puzzle.size[1]))
    width, height = new_img_puzzle.size
    if x1 + width > img_background.size[0]:
        width = img_background.size[0] - x1
    if y1 + height > img_background.size[1]:
        height = img_background.size[1] - y1
    if width <> new_img_puzzle.size[0] or height <> new_img_puzzle.size[1]:
        new_img_puzzle = new_img_puzzle.crop((0, 0, width, height))
    x2, y2 = x1 + new_img_puzzle.size[0], y1 + new_img_puzzle.size[1]
    new_img_background.paste(new_img_puzzle, (x1, y1, x2, y2))#, new_img_puzzle)
    return new_img_background

def getJPEGfileSize(temp_img_dir, pil_img, jpeg_quality = 90):
    temp_img_filename = 'temp' + str(random.randint(100000,999999)) + '.jpg'
    timg_fg = os.path.join(temp_img_dir, temp_img_filename)
    if os.path.isfile(timg_fg):
        os.remove(timg_fg)
    pil_img.save(timg_fg, 'JPEG', quality = jpeg_quality)
    jpegSize = os.path.getsize(timg_fg)    
    borrado = False
    numBorrTries = 0
    while not(borrado) and numBorrTries < 5:
        borrado = True
        try:
            os.remove(timg_fg)
        except:
            borrado = False
        numBorrTries += 1
        if not(borrado):
            time.sleep(0.1)
    return jpegSize

def getMinJPEGfileSize(temp_img_dir, pil_img_fondo, pil_img_pieza_puzzle, jpeg_quality = 100):  
    global img_size
    global puzzle_size
    minh_sz_pil_img_sol = None
    for position_x in range(0, img_size[0] - puzzle_size[0], 10):
        for position_y in range(0, img_size[1] - puzzle_size[1], 10):
            pil_img_sol = mergePNG(pil_img_fondo, pil_img_pieza_puzzle, (position_x, position_y))
            sz_pil_img_sol = getJPEGfileSize(temp_img_dir, pil_img_sol, jpeg_quality)
            if minh_sz_pil_img_sol == None or minh_sz_pil_img_sol > sz_pil_img_sol:
                minh_sz_pil_img_sol = sz_pil_img_sol
                minh_position = (position_x, position_y)   
                mins_pil_img_sol = pil_img_sol
    return minh_position, minh_sz_pil_img_sol, mins_pil_img_sol

main('/home/us/raise-1k-PNGs', '/tmp' , True, '/home/us/capy-experiment', True, True)
