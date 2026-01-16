import os
import shutil
from datetime import datetime


def organize(input_dir, extension, output_dir):
    for filename in os.listdir(input_dir):
        if filename.lower().endswith(tuple(extension)):
            file_path = os.path.join(input_dir, filename)

            create_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            year, month = create_time.strftime('%Y-%m').split('-')
            output_path = os.path.join(output_dir, year, month)
            os.makedirs(output_path, exist_ok=True)

            shutil.move(file_path, os.path.join(output_path, filename))


def image():
    input_dir = r'pass in your directory'
    output_dir = r'pass in your directory'

    image_extensions  = [".jpg", ".png", ".gif", ".jpeg", ".svg"]
    organize(input_dir, image_extensions, output_dir)


def video():
    input_dir = r'pass in your directory'
    output_dir = r'pass in your directory'

    video_extentions = [".mp4", ".avi", ".mov", ".wmv", ".mkv"]
    organize(input_dir, video_extentions, output_dir)


image()
video()