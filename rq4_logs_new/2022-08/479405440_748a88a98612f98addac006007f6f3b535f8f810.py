from PIL import Image
import argparse


def data_bytes_sz(data):
    return len(data.encode('utf-8'))


def img_bytes_sz(image):
    x, y = image.size
    return 3 * x * y


def bytes_to_crypt_byte(data_sz, image_sz):
    return min((image_sz - 4) // data_sz, 8)


def is_steganography_possible(k):
    return k >= 2


def bits_for_crypt(k, current):
    return 8 // k + (1 if current < 8 % k else 0)


def change_pixels(data, img_data, k):
    data = data.encode('utf-8')
    img_data[0] = ((img_data[0] >> 4) << 4) + (k >> 4)
    img_data[1] = ((img_data[0] >> 4) << 4) + (k % 16)

    for i, data_character in enumerate(data):
        for c in range(k):
            index = 2 + i * k + c
            n = bits_for_crypt(k, c)
            img_data[index] = ((img_data[index] >> n) << n) + (data_character >> (8 - n))
            data_character = (data_character << n) % 256

    img_data[2 + len(data) * k] = ((img_data[2 + len(data)] >> 4) << 4)
    img_data[3 + len(data) * k] = ((img_data[3 + len(data)] >> 4) << 4)


def crypt_steganography(data, image_name):
    original = Image.open(image_name, 'r')
    image = original.copy()

    data_sz = data_bytes_sz(data)
    img_sz = img_bytes_sz(image)

    k = bytes_to_crypt_byte(data_sz, img_sz)

    if not is_steganography_possible(k):
        print('Error: picture is too small')

    img_data = [num for pixel in iter(original.getdata()) for num in pixel]
    pixel_channels = len(original.getpixel((0, 0)))

    change_pixels(data, img_data, k)

    cur = 0
    for i in range(original.size[1]):
        for j in range(original.size[0]):
            pixel = (img_data[cur + p] for p in range(pixel_channels))
            image.putpixel((j, i), tuple(pixel))
            cur += pixel_channels

    image.save('with_secret_' + image_name, quality=100)


def get_characters(img_data, k):
    start = 2
    while True:
        character = 0
        for i in range(k):
            n = bits_for_crypt(k, i)
            character = character << n
            character += (img_data[start + i] % (1 << n))
        start += k
        if character == 0:
            return
        yield character


def steganography_decrypt(image_name):
    image = Image.open(image_name, 'r')
    img_data = [num for pixel in iter(image.getdata()) for num in pixel]

    k = (img_data[0] << 4) + (img_data[1] % (1 << 4))

    byte_str = bytes(iter(get_characters(img_data, k)))

    print(byte_str.decode('utf-8'))