import argparse
import sys
import os
sys.path.append(os.path.abspath('../'))
from src import utils
from src.screen import Screen
from src.utils import OutputType

def save_screens(log, half=False):
    screens = []
    for req in log.requests:
        if req.data_type == OutputType.Screen:
            screens.append(req.values[0])
    i = 0
    for screen in screens:
        if half:
            buff = screen.get_box(0,0,screen.shape.width//2,screen.shape.height)
            screen = Screen(buff=buff)
        path = "screen{:03d}.png".format(i)
        i += 1
        screen.to_image(save=path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", help="Path of log")
    args = parser.parse_args()
    log = utils.load(args.log)
    save_screens(log, True)