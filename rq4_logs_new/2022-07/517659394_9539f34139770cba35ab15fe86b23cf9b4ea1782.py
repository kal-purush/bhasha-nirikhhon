import os
import requests
import pyperclip

###
# Class Scraper demonstrates the use of loading content from a web page


class Scraper:
    ###
    # Class members.
    # _text: this is the text that was scraped

    ###
    # Class constructor.
    # source: indicates the source to use for scraping the text:
    #         if source starts with "http", text is loaded from a web page
    #            (i.e. https://www.gutenberg.org/files/147/147-0.txt)
    #         if source points to a file on the disk, text is loaded from that file
    #         if source is any other string, text is loaded directly from there
    #         otherwise the text is loaded from the clipboard
    def __init__(self, source=None):
        # Loads the internal _text from various sources
        if source:
            if source.startswith("http"):
                response = requests.get(source)
                self._text = response.text
            elif os.path.isfile(source):
                self._text = open(source).read()
            else:
                self._text = source
        else:
            self._text = pyperclip.paste()

    def __str__(self):
        return f"Scraped text: '{self._text}'"