import requests
from PIL import Image
from io import BytesIO
import regex
def get_image(url):
    r = requests.get(url)
    content = r.content
    print(r.status_code)
    bio = BytesIO()
    bio.write(content)
    bio.seek(0)
    return Image.open(bio)
class EmojiFail(Exception):
    pass
pattern = r"\p{Emoji_Presentation}"
_re = regex.compile(pattern)
def get_emoji(code = "1F975"):
    try:
        url = "https://openmoji.org/php/download_asset.php?type=emoji&emoji_hexcode=%s&emoji_variant=color"%(code.upper(),)
        im = get_image(url)
    except Exception as e:
        raise EmojiFail("Cannot get for %s"%code)
    return im
if(__name__=="__main__"):
    im = get_emoji("1f123")
    pth = "/home/TkskKurumi/tmp/tmp.png"
    im.save(pth)
    print(pth)