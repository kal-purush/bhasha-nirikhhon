# Library import

from requests import Request, Session
import re, os
import base64


# Challenge URL
url = "http://challenge01.root-me.org/programmation/ch8/"

# Regex used for b64 img grab
regex=".*image/png;base64,(?P<image>.*)\" /><br>.*"
responseRegex=".*Leaking solutions on Internet is forbidden</a></p><p>(?P<response>.*)<br></p><br/>.*"
p = re.compile(regex)
p2 = re.compile(responseRegex)


result = 0

while result != 1:
    # HTTP session
    s = Session()

    # GET Method
    r = s.get(url,stream=True)

    # Save cookie to injeect in the response
    c = r.cookies

    # Save the response to the GET request
    response = r.content

    # Grab b64 img in the response
    m = p.match(response)
    # print "b64 img:" + m.group("image")
    captchab64 = m.group("image")

    # Convert b64 image to png file
    captcha_file = open('captcha.png', 'wb')
    captcha_file.write(base64.decodestring(captchab64))
    captcha_file.close()

    # Analyse png file to found text flag and store it in text.txt file
    command = "tesseract -psm 8 captcha.png text alphadigits"

    os.system(command)

    f = open('text.txt', 'r')
    captcha = f.read().strip("\n")
    print "result: " + captcha
    f.close()

    r = s.post(url, cookies = c, verify = False, data={'cametu':captcha})
    # print r.content

    # print m2.group("response")
    try:
        m2 = p2.match(r.content)
    except NameError:
        result = 1
        print r.content