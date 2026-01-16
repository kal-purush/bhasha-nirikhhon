import os
import base64
import hashlib
import random

from fil3_functions import generate_random_bytes_file


def create_dummy_file(file_path, size):
    with open(file_path, 'wb') as dummy_file:
        dummy_file.write(os.urandom(size))


def create_corrupted_file(file_path):
    with open(file_path, 'wb') as corrupted_file:
        corrupted_file.write(b'corrupted data')


def corrupt_existing_file(file_path):
    with open(file_path, 'r+b') as file:
        file.seek(random.randint(0, os.path.getsize(file_path) - 1))
        file.write(b'\x00')




generate_random_bytes_file()