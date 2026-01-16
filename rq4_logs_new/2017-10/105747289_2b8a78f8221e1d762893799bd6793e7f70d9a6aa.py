from PIL import Image
from PIL import ImageFont
from PIL import ImageDraw

img = Image.open("gift.png")
draw = ImageDraw.Draw(img)
# font = ImageFont.truetype(<font-file>, <font-size>)
font = ImageFont.truetype("THSarabunNew.ttf", 36)
# draw.text((x, y),"Sample Text",(r,g,b))
draw.text((0, 0), "สวัสดีครับสั้นส้นกตัญญูรู้คุรา่ฟสดากฟหฟกดหฟกดกหฟด\nkjdkljflkdjfkljdklfjdlksf",
          '#123456', font=font)
img.save('sample-out.jpg')