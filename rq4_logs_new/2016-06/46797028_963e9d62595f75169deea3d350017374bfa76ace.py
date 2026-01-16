from PIL import Image, ImageDraw, ImageFont

def make(text, filename):
    image_width = len(text) * 14
    image_height = 27
    color = (0,0,0,255)
    font = ImageFont.truetype('Ubuntu-B.ttf', 24)

    txt = Image.new('RGBA', (image_width, image_height), (255,255,255,255))
    d = ImageDraw.Draw(txt)
    d.text((0,0), text, font=font, fill=color)
    # out = Image.alpha_composite(txt, txt)
    txt.save(filename, "png")

make("5579", "result.png")