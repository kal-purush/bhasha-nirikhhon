from PIL import Image
import re
from requests import Session
import base64

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
qrcode = m.group("image")

# Convert b64 image to png file
qrcodeImg = open('qrcode.png', 'wb')
qrcodeImg.write(base64.decodestring(qrcode))
qrcodeImg.close()


# Merge qrcode with base QRCode

qrcode="test"
r = s.post(url, cookies=c, verify=False, data={'metu': qrcode})

if p3.match(r.content):
    print "Flag: " + p3.match(r.content).group("flag")
else :
    print "try again"
    print r.content