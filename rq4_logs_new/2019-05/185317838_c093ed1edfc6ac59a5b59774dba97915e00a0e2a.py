import os
import sys
import time

try:
    from PIL import Image
except ImportError:
    sys.exit("Cannot import from PIL: Do `pip3 install --user Pillow` to install")

import anki_vector
from anki_vector.util import degrees


def main():
    args = anki_vector.util.parse_command_args()
    with anki_vector.Robot(args.serial) as robot:

        robot.behavior.set_head_angle(degrees(45.0))
        robot.behavior.set_lift_height(0.0)

        duration_s = 56.5

        image_file = Image.open("love.jpg")
        screen_data = anki_vector.screen.convert_image_to_screen_data(image_file)

        robot.screen.set_screen_with_image_data(screen_data, duration_s)
        robot.audio.stream_wav_file("love.wav", 75)

if __name__ == "__main__":
    main()