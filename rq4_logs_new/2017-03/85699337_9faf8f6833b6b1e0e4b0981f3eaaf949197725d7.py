from PIL import Image
import zbarlight
import re
from requests import Session
import base64
import qrcode


# Get QR Code Image from root-me

# Challenge URL
url = "http://challenge01.root-me.org/programmation/ch7/"

regex=".*image/png;base64,(?P<image>.*)\" /><br/>.*"
p = re.compile(regex)

flagRegex=".*Congratz, le flag est (?P<flag>.*)</p></p>.*"
p3 = re.compile(flagRegex)


# HTTP session
s = Session()

# GET Method
r = s.get(url, stream=True)

# Save cookie to injeect in the response
c = r.cookies

# Save the response to the GET request
response = r.content

# Grab b64 img in the response
m = p.match(response)
# print "b64 img:" + m.group("image")
qrcodeChall = m.group("image")

# Convert b64 image to png file
qrcodeImg = open('qrcode.png', 'wb')
qrcodeImg.write(base64.decodestring(qrcodeChall))
qrcodeImg.close()


# START TESTING
# Test QRCODE reading
qrcodeTest = open('test.png','rb')
qrcodeTestImg = Image.open(qrcodeTest)
qrcodeTestImg.load()
print zbarlight.scan_codes('qrcode',qrcodeTestImg)

test = qrcode.make('test')
# END OF TEST



# Merge qrcode with base QRCode
baseQR = Image.open('baseqr.png')
baseQR = baseQR.convert("RGBA")

challQR = Image.open('qrcode.png')
challQR = challQR.convert("RGBA")
final = Image.blend(challQR,baseQR,0.5).convert("L")
final = final.point(lambda x: 0 if x<200 else 255, '1')
final.save('final.png')
width, height = final.size

finalQR = open('final.png','rb')
finalQRImg = Image.open(finalQR)
finalQRImg.load()

print zbarlight.scan_codes('qrcode',final)
# print zbarlight.qr_code_scanner(final.tobytes(),width,height)

qrcodeResponse = "test"
r = s.post(url, cookies=c, verify=False, data={'metu': qrcodeResponse})

if p3.match(r.content):
    print "Flag: " + p3.match(r.content).group("flag")
else :
    print "try again"
    print r.content