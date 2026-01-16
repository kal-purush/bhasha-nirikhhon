import qrcode
#PIL is use to formating of Qr code emage
from PIL import Image
qr=qrcode.QRCode(version=1,
                 error_correction=qrcode.constants.ERROR_CORRECT_H,
                 box_size=10,
                 border=4,)#QRcode funtion ko change kerta hai
qr.add_data("https://gyanendra2003.github.io/Portfolio_new")
qr.make(fit=True)
img=qr.make_image(fill_color="green",back_color="white")
img.save("Gyanendra_portfolio.png")