# %%
"""
HDR stencil code - main.py
CS 1290 Computational Photography, Brown U.
"""

import os
import sys
import argparse
import cv2
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use the 'Agg' backend for non-interactive plotting

from mpl_toolkits.axes_grid1 import make_axes_locatable

from utils import parse_files, create_sample_matrix, plot_g, tm_global_scale
from student import solve_g, hdr, tm_global_simple, tm_durand, reinhard_local_tone_mapping

def main():
    """Main function."""

    # PARSE ARGUMENTS
    # ===============================
    arg_parser = argparse.ArgumentParser(description="CS1290: Project 2 - HDR")
    arg_parser.add_argument("-d", "--dir", type=str, default="../data/arch",
                            help='''Image set to perform HDR tone mapping on. This can either
                            be a full path to the directory of exposure images, or the name of
                            a folder in ../data/''')
    arg_parser.add_argument("-l", "--lambda", type=float, default=50.0,
                            help="Lambda smoothing factor (default: 50.0).")
    arg_parser.add_argument("-o", "--out", type=str, default="./results/",
                            help="Directory in which to save output images (default: ./results)")
    args = arg_parser.parse_args()

    # Make sure input directory exists
    directory = args.dir
    if not os.path.isdir(directory):
        directory = os.path.join("../", os.path.join("data", directory))
        if not os.path.isdir(directory):
            print("Directory does not exist!")
            sys.exit(1)

    # READ IMAGE STACK INFO
    # ===============================
    # MY CODE:
    print("args.out: ", args.out)

    image_name = os.path.basename(directory)
    print("image_name: ", image_name)

    # END MY CODE

    file_names, exposures, nr_exposures = parse_files(directory)

    tmp = cv2.imread(file_names[0])
    nr_pixels = tmp.shape[0] * tmp.shape[1]

    print("nr_exposures: {}".format(nr_exposures))
    print("nr_pixels: {}".format(nr_pixels))


    # GENERATE HDR RADIANCE MAP
    # ===============================

    print("Generating HDR radiance map...")

    # load and sample the images
    sample_matrix = create_sample_matrix(file_names, nr_pixels, nr_exposures)

    # create exposure matrix
    exposure_matrix = np.zeros((sample_matrix.shape[0] * sample_matrix.shape[1], nr_exposures))
    for i in range(nr_exposures):
        exposure_matrix[:, i] = np.log(exposures[i])

    # lambda smoothing factor
    l = vars(args)["lambda"]
    
    # weights
    t = np.arange(1, 129)
    weights = np.concatenate([t, t[::-1]], axis=0)

    # solve for g in each color channel
    # WE IMPLEMENT SOLVE_G FUNCTION IN STUDENT.PY
    g_red, le_red = solve_g(sample_matrix[..., 0], exposure_matrix, l, weights)
    g_green, le_green = solve_g(sample_matrix[..., 1], exposure_matrix, l, weights)
    g_blue, le_blue = solve_g(sample_matrix[..., 2], exposure_matrix, l, weights)

    # compute the hdr radiance map
    # WE IMPLEMENT HDR FUNCTION IN STUDENT.PY  
    hdr_radiance_map = hdr(file_names, g_red, g_green, g_blue, weights, exposure_matrix, nr_exposures)

    # plot g curves
    plt.figure()
    plot_g(g_red, g_green, g_blue)
    plt.savefig(os.path.join(args.out, image_name + "_g_curves.png"))
    plt.close()
    # save g curves


    # PLOT RADIANCE MAPS
    # ================================

    if not os.path.isdir(args.out):
        os.makedirs(args.out, exist_ok=True)

    plt.figure(figsize=(13, 6))

    rad_map = np.log(hdr_radiance_map+1)
    mean_rad_map = rad_map.mean(axis=-1)

    plt.subplot(1, 2, 1)
    plt.title("HDR Radiance Map (mean of channels)")
    plt.axis('off')
    ax = plt.gca()
    rad_map_im = ax.imshow(mean_rad_map, cmap='jet')
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    plt.colorbar(rad_map_im, cax=cax)
    plt.imsave(os.path.join(args.out, image_name + "_hdr_radiance_map_mean.png"), mean_rad_map, cmap='jet')

    rad_map = np.clip(rad_map, 0., 1.)

    plt.subplot(1, 2, 2)
    plt.imshow(rad_map)
    plt.axis('off')
    plt.title("HDR Radiance Map")
    plt.imsave(os.path.join(args.out, image_name + "_hdr_radiance_map.png"), rad_map)

    plt.tight_layout()
    # plt.show()


    # PLOT TONE MAP
    # ===============================

    print("Tone mapping...")

    # WE IMPLEMENT TM_GLOBAL_SCALE FUNCTION IN STUDENT.PY
    global_scale = tm_global_scale(hdr_radiance_map)

    # WE IMPLEMENT TM_GLOBAL_SIMPLE FUNCTION IN STUDENT.PY
    global_simple = tm_global_simple(hdr_radiance_map)

    # WE IMPLEMENT TM_DURAND FUNCTION IN STUDENT.PY
    durand, bilateral, base, detail = tm_durand(hdr_radiance_map)

    # Plot global scale tone mapping
    plt.figure(figsize=(13, 6))
    plt.subplot(1, 3, 1)
    plt.imshow(global_scale)
    plt.axis('off')
    plt.title("Global Scale")
    cv2.imwrite(os.path.join(args.out, image_name + "_global_scale.png"),
                global_scale[..., ::-1]*255)

    # Plot global simple tone mapping
    plt.subplot(1, 3, 2)
    plt.imshow(global_simple)
    plt.axis('off')
    plt.title("Global Simple")
    cv2.imwrite(os.path.join(args.out, image_name + "_global_simple.png"),
                global_simple[..., ::-1]*255)

    # Plot Durand tone mapping
    plt.subplot(1, 3, 3)
    plt.imshow(durand)
    plt.axis('off')
    plt.title("Durand")
    cv2.imwrite(os.path.join(args.out, image_name + "_durand.png"),
                durand[..., ::-1]*255)
    # close plot
    plt.close()
    
    # NEW CODE

    # Reinhard Local Tone Mapping
    reinhard = reinhard_local_tone_mapping(hdr_radiance_map)
    plt.subplot(1, 3, 3)
    plt.imshow(reinhard)
    plt.axis('off')
    plt.title("Reinhard")
    cv2.imwrite(os.path.join(args.out, image_name + "_reinhard.png"),
                reinhard[..., ::-1]*255)
    
    # TODO: Bilateral filter, base, detail

    # bilateral filter
    plt.figure()
    plt.imshow(bilateral)
    plt.axis('off')
    plt.title("Bilateral")
    cv2.imwrite(os.path.join(args.out, image_name + "_bilateral.png"),
                bilateral[..., ::-1]*255)
    plt.close()
    # base
    plt.figure()
    plt.imshow(base)
    plt.axis('off')
    plt.title("Base")
    cv2.imwrite(os.path.join(args.out, image_name + "_base.png"),
                base[..., ::-1]*255)
    plt.close()

    # detail
    plt.figure()
    plt.imshow(detail)
    plt.axis('off')
    plt.title("Detail")
    cv2.imwrite(os.path.join(args.out, image_name + "_detail.png"),
                detail[..., ::-1]*255)
    plt.close()
    plt.tight_layout()
    # plt.show()

def run_all_data_directories():
    import os
    import sys

    data_dir = "../data"
    subdirs = [
        os.path.join(data_dir, d)
        for d in os.listdir(data_dir)
        if os.path.isdir(os.path.join(data_dir, d))
    ]

    original_args = sys.argv.copy()

    for subdir in subdirs:
        print("subdir: ", subdir)
        sys.argv = [original_args[0], "--dir", subdir]
        main()

    sys.argv = original_args

run_all_data_directories()

# %%