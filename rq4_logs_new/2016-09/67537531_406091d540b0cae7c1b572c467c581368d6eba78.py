"""Weather Maniac optical character recognition functions."""

from PIL import Image, ImageOps, ImageEnhance
import subprocess
import io
import re

TESSERACT_EXE_NAME = r"c:/users/eric/desktop/weatherman/tesseract.exe"


JPG_FEATURES = {
    'x_pitch': 86.5,
    'x_start': 98,
    'max_loc_y': 301,
    'min_loc_y': 334,
    'day_loc_y': 127,
    'max_win_x': 81,
    'max_win_y': 37,
    'min_win_x': 54,
    'min_win_y': 28,
    'day_win_x': 73,
    'day_win_y': 18
}

TEMP_RE = re.compile('-?\d{1,3}')
DAY_RE = re.compile('\w{3}')
O_RE = re.compile('O')
I_RE = re.compile('I')
FHI_RE = re.compile('FHI')



def max_crop_dim(day_num):
    """Return the max temp crop window.

    day_num holds the day (column) number, 0-referenced.
    """
    x_index = JPG_FEATURES['x_pitch'] * day_num + JPG_FEATURES['x_start']
    x_min = round(x_index - JPG_FEATURES['max_win_x'] / 2)
    y_min = round(JPG_FEATURES['max_loc_y'] - JPG_FEATURES['max_win_y'] / 2)
    x_max = round(x_index + JPG_FEATURES['max_win_x'] / 2)
    y_max = round(JPG_FEATURES['max_loc_y'] + JPG_FEATURES['max_win_y'] / 2)
    return x_min, y_min, x_max, y_max


def min_crop_dim(day_num):
    """Return the max temp crop window.

    day_num holds the day (column) number, 0-referenced.
    """
    x_index = JPG_FEATURES['x_pitch'] * day_num + JPG_FEATURES['x_start']
    x_min = round(x_index - JPG_FEATURES['min_win_x'] / 2)
    y_min = round(JPG_FEATURES['min_loc_y'] - JPG_FEATURES['min_win_y'] / 2)
    x_max = round(x_index + JPG_FEATURES['min_win_x'] / 2)
    y_max = round(JPG_FEATURES['min_loc_y'] + JPG_FEATURES['min_win_y'] / 2)
    return x_min, y_min, x_max, y_max


def day_crop_dim(day_num):
    """Return the max temp crop window.

    day_num holds the day (column) number, 0-referenced.
    """
    x_index = JPG_FEATURES['x_pitch'] * day_num + JPG_FEATURES['x_start']
    x_min = round(x_index - JPG_FEATURES['day_win_x'] / 2)
    y_min = round(JPG_FEATURES['day_loc_y'] - JPG_FEATURES['day_win_y'] / 2)
    x_max = round(x_index + JPG_FEATURES['day_win_x'] / 2)
    y_max = round(JPG_FEATURES['day_loc_y'] + JPG_FEATURES['day_win_y'] / 2)
    return x_min, y_min, x_max, y_max


def crop_image():
    """  """
    img = Image.open('C:/Users/Eric/weather_maniac/rawdatafiles/Screen_Data/screen_2016_09_28.jpg')
    for day_num in range(7):
        box = max_crop_dim(day_num)
        small_img = img.crop(box)
        small_img.load()
        small_gr_img = ImageEnhance.Color(small_img).enhance(0)
        small_bw_img = ImageEnhance.Contrast(small_gr_img).enhance(2)
        pad_img = ImageOps.expand(small_bw_img, border=20, fill='black')
        output = io.BytesIO()
        pad_img.save(output, format='JPEG')
        pad_jpg = output.getvalue()
        output.close()
        out_string = call_tesseract(pad_jpg)
        out_string= I_RE.sub('1', out_string)
        out_string= O_RE.sub('0', out_string)
        if TEMP_RE.match(out_string):
            out_string = TEMP_RE.match(out_string).group(0)
        else:
            out_string = ''
        print(out_string)

        box = min_crop_dim(day_num)
        small_img = img.crop(box)
        small_img.load()
        small_gr_img = ImageEnhance.Color(small_img).enhance(0)
        small_bw_img = ImageEnhance.Contrast(small_gr_img).enhance(2)
        pad_img = ImageOps.expand(small_bw_img, border=20, fill='black')
        output = io.BytesIO()
        pad_img.save(output, format='JPEG')
        pad_jpg = output.getvalue()
        output.close()
        out_string = call_tesseract(pad_jpg)
        out_string= I_RE.sub('1', out_string)
        out_string= O_RE.sub('0', out_string)
        if TEMP_RE.match(out_string):
            out_string = TEMP_RE.match(out_string).group(0)
        else:
            out_string = ''
        print(out_string)


        box = day_crop_dim(day_num)
        small_img = img.crop(box)
        small_img.load()
        small_gr_img = ImageEnhance.Color(small_img).enhance(0)
        small_bw_img = ImageEnhance.Contrast(small_gr_img).enhance(2)
        pad_img = ImageOps.expand(small_bw_img, border=20, fill='black')
        output = io.BytesIO()
        pad_img.save(output, format='JPEG')
        pad_jpg = output.getvalue()
        output.close()
        out_string = call_tesseract(pad_jpg)
        out_string= FHI_RE.sub('FRI', out_string)
        if DAY_RE.match(out_string):
            out_string = DAY_RE.match(out_string).group(0)
        else:
            out_string = ''
        print(out_string)



def call_tesseract(input_image):
    """Calls external tesseract.exe on input file (restrictions on types),
    outputting output_filename+'txt'"""
    # with open('max_0.jpg', 'rb') as f:
    #     input_image=f.read()
    args = [TESSERACT_EXE_NAME, 'stdin', 'stdout']
    print(args)
    proc = subprocess.Popen(args,
                            stdout=subprocess.PIPE,
                            stdin=subprocess.PIPE)
    data, error = proc.communicate(input=input_image)
    print(data, error)
    return data.decode('utf-8')


def main():
    crop_image()


if __name__ == '__main__':
    main()
