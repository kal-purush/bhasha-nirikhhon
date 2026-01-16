import glob, os
import config
from common import memoize, wrap_output

@wrap_output(list)
def list_all(collection = None):
    if collection:
        for i in glob.glob(os.path.join(config.imgdir, collection, "*.png")):
            yield os.path.join(collection, os.path.basename(i))
        for i in glob.glob(os.path.join(config.imgdir, collection, "*.gif")):
            yield os.path.join(collection, os.path.basename(i))
        for i in glob.glob(os.path.join(config.imgdir, collection, "*.jpg")):
            yield os.path.join(collection, os.path.basename(i))
    else:
        for i in glob.glob(os.path.join(config.imgdir, "*.png")):
            yield os.path.basename(i)
        for i in glob.glob(os.path.join(config.imgdir, "*.gif")):
            yield os.path.basename(i)
        for i in glob.glob(os.path.join(config.imgdir, "*.jpg")):
            yield os.path.basename(i)

@wrap_output(list)
def list_collections():
    for i in glob.glob(os.path.join(config.imgdir, "*")):
        if not (i.endswith(".png") or i.endswith(".gif") or i.endswith(".jpg")):
            yield os.path.basename(i)


@memoize
def get(name, filetype):
    with open(os.path.join(config.imgdir, f"{name}.{filetype}"), "rb") as f:
        return f.read()

def store(name, data):
    with open(os.path.join(config.imgdir, f"{name}"), "wb" if type(data) is bytes else "w") as f:
        return f.write(data)