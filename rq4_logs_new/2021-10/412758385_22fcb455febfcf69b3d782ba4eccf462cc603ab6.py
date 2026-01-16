import os

def clr_dir(src_dir_path):
    try:
        items = os.listdir(src_dir_path)
        for item in items:
            os.remove(src_dir_path+'/'+item)

    except:
        return