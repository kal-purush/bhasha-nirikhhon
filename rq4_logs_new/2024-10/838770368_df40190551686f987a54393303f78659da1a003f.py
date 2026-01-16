#!/usr/bin/env python3

import cv2
import math
from deskew import determine_skew
import numpy as np
import pathlib


def deskew_image(img_path):
    im = cv2.imread(img_path, cv2.IMREAD_GRAYSCALE)
    angle = determine_skew(im)

    if angle <= -3.5 or angle >= 3.5:
        return im, angle

    old_w, old_h = im.shape
    rad = math.radians(angle)
    w = abs(np.sin(rad) * old_h) + abs(np.cos(rad) * old_w)
    h = abs(np.sin(rad) * old_w) + abs(np.cos(rad) * old_h)

    center = tuple(np.array(im.shape[1::-1]) / 2)
    matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    new = cv2.warpAffine(im, matrix, (int(round(h)), int(round(w))))
    return new, angle


def process_images(input_images, output_directory):
    """Apply transformations to all images.

    Processes all images in input_images, applies transformations and saves the
    resulting images as PNG files output_directory.

    Args:
        input_images: An iterable containing paths of .tif images.
        output_directory: A pathlib.Path where PNG images will be saved."""

    output_directory = pathlib.Path(output_directory)
    assert output_directory.is_dir()

    for img_path in input_images:
        output_img, angle = deskew_image(img_path)
        output_path = output_directory / img_path.with_suffix('.png').name
        cv2.imwrite(output_path, output_img)


if __name__ == '__main__':
    import os
    import sys
    import argparse
    parser = argparse.ArgumentParser(description='Preprocess TIFF images in directory and save as PNG files')
    parser.add_argument(
        '-i',
        '--input',
        required=True,
        help='Input directory',
        type=pathlib.Path
    )
    parser.add_argument(
        '-o',
        '--output',
        required=True,
        help='Output directory',
        type=pathlib.Path
    )
    # TODO: add -j argument and add ability to run in parallel
    args = parser.parse_args()

    if not args.input.exists():
        sys.stderr.write(f"Error: Directory {args.input} does not exist\n")
        exit(1)

    if not args.input.is_dir():
        sys.stderr.write(f"Error: {args.input} is not a directory\n")
        exit(1)

    os.makedirs(args.output, exist_ok=True)

    from tqdm import tqdm
    input_images = tqdm(sorted(args.input.glob('*.tif')))
    process_images(input_images, args.output)