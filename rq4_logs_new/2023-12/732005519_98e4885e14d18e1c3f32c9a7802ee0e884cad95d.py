import hashlib


def get_md5_hash(file_path):
    with open(file_path, 'rb') as opened_file:
        content = opened_file.read()
        md5 = hashlib.md5()
        md5.update(content)
        return md5.hexdigest()