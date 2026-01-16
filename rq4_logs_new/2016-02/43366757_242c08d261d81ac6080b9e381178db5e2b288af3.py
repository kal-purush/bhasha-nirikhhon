#!/usr/bin/python
"""
Paste clipboard buffer to pastebin.

Takes the contents of the clipboard and creates a new paste on http://pastebin.com
and then puts the resulting url in the clipboard buffer.
Requires a pastebin API developer key
"""

import pyperclip
import argparse
import urllib.request
import urllib.parse

PASTEBIN_URL = "http://pastebin.com/api/api_post.php"

def paste(data, dev_key, pastebin_url):
    pastebin_vars = dict(
        api_option = "paste",
        api_dev_key = dev_key,
        api_paste_code = data
    )
    paste_data = urllib.parse.urlencode(pastebin_vars).encode("utf8")
    return urllib.request.urlopen(pastebin_url, paste_data).read()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Pastebin paste")
    parser.add_argument("-d", "--devkey", help="Your Pastebin developer key", required=True)

    args = parser.parse_args()
    dev_key = args.devkey
    paste_data = pyperclip.paste()

    url = paste(paste_data, dev_key, PASTEBIN_URL)

    pyperclip.copy(url.decode("ascii"))
