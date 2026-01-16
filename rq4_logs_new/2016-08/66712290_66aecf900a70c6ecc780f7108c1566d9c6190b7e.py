# capture images and dream them

import sys
import time
import random
import os.path
import cv2
import numpy as np
import PIL.Image

# suppress log spam from caffe
# http://stackoverflow.com/questions/29788075/setting-glog-minloglevel-1-to-prevent-output-in-shell-from-caffe
os.putenv('GLOG_minloglevel', '2')

import dreamify

# TWEAKABLES

# camera
camera_idx = 0
flash_intensity = 150

# starting screen
is_fullscreen = 0

# deepdream
models = ['slugdog', 'places']
layers = [
    # googlenet (slugdog)
    ['conv2/3x3_reduce'],
    #['pool3/3x3_s2', 'inception_4a/output', 'inception_4b/output', 'inception_4c/5x5_reduce', 'inception_4c/output'],
    # places
    []
    #['inception_4b/5x5_reduce', 'inception_4b/5x5', 'inception_4b/pool', 'inception_4b/pool_proj'],
]
iter_n = 5
octaves = 4
oct_scale = 1.4

# some good ones:
# LINES: conv2/3x3_reduce
# SPIRALS: slugdog, inception_3b/3x3_reduce
# ARCHES: places, inception_4a/output

# dimensions
process_h = 720
render_h = 720
init_render_w = 1280

# progress circle
circle_alpha = .4
full = (0, 0, 0)
empty = (255, 255, 255)

# saves
save_dir = r"saves"
save_stamp = "%Y-%m-%d %H-%M-%S"
save_format = "jpeg"

# END TWEAKABLES

name = "Outside"
name2 = "Inside"

def get_camera():
    cap = cv2.VideoCapture(camera_idx)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
    return cap

def create_sized_window(n):
    cv2.imshow(n, flash)

def set_is_fullscreen():
    cv2.setWindowProperty(name, cv2.WND_PROP_FULLSCREEN, is_fullscreen)

def toggle_fullscreen():
    global is_fullscreen
    is_fullscreen = 1 - is_fullscreen
    set_is_fullscreen()
    wait_key()

def wait_key(delay=1):
    key = cv2.waitKey(delay)
    if key == ord('q'):
        sys.exit()
    elif key == ord('f'):
        toggle_fullscreen()

def resize_to_h(im, target_h):
    h, w = im.shape[:2]
    target_w = int(w * float(target_h) / h)
    return target_w, cv2.resize(im, (target_w, target_h))

def prime_camera(cap):
    cap.read()
    cap.read()

flash = flash_intensity * np.ones((render_h, init_render_w, 3), np.uint8)
def snap(cap):
    hour = time.localtime().tm_hour
    do_flash = (hour >= 20) or (hour < 6)
    if do_flash:
        cv2.imshow(name, flash)
        wait_key(200)
    cap.read()
    _, image = cap.read()
    return image

def main_loop(cap):
    while True:
        source = snap(cap)
        source = cv2.flip(source, 1)
        source_h, source_w = source.shape[:2]
        print source.shape

        process_w, process_input = resize_to_h(source, process_h)

        last_two_times = [time.time()] * 2

        def callback(progress, image):
            global is_fullscreen
            render_w, render_i = resize_to_h(np.uint8(np.clip(image, 0, 255)),
                                             render_h)
            circle_i = render_i.copy()
            unit = render_h / 20
            center = (render_w / 2, render_h - unit * 2)
            radius = unit
            thickness = radius / 3
            angle = progress * 360
            if angle <= 359:
                cv2.ellipse(circle_i, center, (radius, radius), -90, angle,
                            360, empty, thickness, cv2.LINE_AA)
            if progress:
                cv2.ellipse(circle_i, center, (radius, radius), -90, 0, angle,
                            full, thickness, cv2.LINE_AA)

            render = cv2.addWeighted(render_i, 1 - circle_alpha, circle_i,
                                               circle_alpha, 0)

            cv2.imshow(name2, render)
            wait_key()
            cv2.imshow(name, render)
            wait_key()

            last_two_times.pop(0)
            last_two_times.append(time.time())

        callback(0, source)
        desc, result = process_image(process_input, callback)

        desc = desc.replace('/', '-')
        filename = "%s.%s.%s" % (time.strftime(save_stamp), desc, save_format)
        filepath = os.path.join(save_dir, filename)
        with open(filepath, 'wb') as fp:
            # opencv uses bgr order, pil assumes rgb
            rgb = cv2.cvtColor(result, cv2.COLOR_BGR2RGB)
            PIL.Image.fromarray(rgb).save(fp, save_format)

        last_timestep = last_two_times[1] - last_two_times[0]
        time.sleep(1)
        wait_key()

last_net_layer_id = None
def process_image(source, callback):
    global last_net_layer_id
    net_layer = random.choice(nets_layers)
    if len(nets_layers) > 1:
        while id(net_layer) == last_net_layer_id:
            net_layer = random.choice(nets_layers)
    last_net_layer_id = id(net_layer)
    net, layer = net_layer
    float_result = dreamify.deepdream(
            net, source, end=layer, iter_n=iter_n, octave_n=octaves,
            octave_scale=oct_scale, callback=callback, clip=True
    )
    return layer, np.uint8(np.clip(float_result, 0, 255))

if __name__ == '__main__':
    # opencv uses bgr order
    nets = [dreamify.load_net(model_name, bgr=True) for model_name in models]
    nets_layers = [(net, layer) for net, net_layers in zip(nets, layers)
                                for layer in net_layers]
    if not nets_layers:
        nets_layers = [
                (net, layer)
                for net in nets
                for layer in net.blobs.keys()
                if layer.startswith('inception')
                    and not layer.startswith('inception_5')
                and 'split' not in layer
        ]
    create_sized_window(name)
    create_sized_window(name2)
    set_is_fullscreen()
    cap = get_camera()
    prime_camera(cap)
    main_loop(cap)