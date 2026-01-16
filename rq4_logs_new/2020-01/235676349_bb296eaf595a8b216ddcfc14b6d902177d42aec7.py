from snake.scale import FileType, scale


NAME = "bitmap"
VERSION = "0.1"

AUTHOR = "Peter Zuidema"
AUTHOR_EMAIL = "peter@icebear.net"

DESCRIPTION = "a module to create a bitmap of the malware sample"

LICENSE = "https://github.com/srcr/bitmap-scale/blob/master/bitmap-scale/LICENSE"

URL = "https://github.com/srcr/bitmap-scale"


__scale__ = scale(
    name=NAME,
    description=DESCRIPTION,
    version=VERSION,
    author=AUTHOR,
    supports=[
        FileType.FILE
    ],
)