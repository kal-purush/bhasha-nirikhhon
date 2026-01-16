import os
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="generating links to images given directory")
    parser.add_argument("--imgDir", "-d", type=str, required=True, help="path to target directory")
    return parser.parse_args()



if __name__ == "__main__":
    args = parse_args()
    img_dir = args.imgDir.rstrip('/')
    img_subdirs = []
    for it in os.scandir(img_dir):
        if it.is_dir():
            img_subdirs.append(it.name)
    images = [img for img in os.listdir(img_dir) if img.endswith('.png') or img.endswith('.jpg') or img.endswith('.jpeg') or img.endswith('.mp4') or img.endswith('.svg')]
    img_urls = ["https://github.com/realzza/blog-images/blob/main/%s/%s?raw=true"%(img_dir, img) for img in images]
    if img_subdirs:
        for idir in img_subdirs:
            sub_images = [img for img in os.listdir(os.path.join(img_dir, idir))]
            images += sub_images
            img_urls += [f"https://github.com/realzza/blog-images/blob/main/{img_dir}/{idir}/{img}?raw=true" for img in sub_images]

    for iu in img_urls:
        print(iu)