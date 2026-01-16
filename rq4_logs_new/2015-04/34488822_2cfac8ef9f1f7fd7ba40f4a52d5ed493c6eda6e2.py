#!/usr/bin/python3 --
# Pyrtal is a file fetching proxy
# TODO:
##1. Check whether python3 is complied with openssl (will broken if with schema https)
##2. Support downloading with user inputed ftp username and password

from flask import Flask, url_for, redirect
import urllib.request
import shutil
import ftplib

# fireup our flask app ;)
app = Flask(__name__)

@app.route('/<path:url>')
## TODO-1
def portal(url):
    with urllib.request.urlopen(url) as response, open('/var/tmp/' + 'ramdom-bits', 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
    return redirect('http://localhost/' + 'ramdom-bits', code=301)


# TODO-2
@app.route('/ftp')
def ftpportal():
    ftp = ftplib.FTP('ftp.kernel.org', 'anonymous', 'pyrtal@')
    ftp.cwd("/pub/linux/kernel")
    ftp.retrlines('RETR ' + 'README', open('/var/tmp/KREADME', 'a+').write)
    return redirect('http://localhost/getfiles/KREADME', code=301)


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1')