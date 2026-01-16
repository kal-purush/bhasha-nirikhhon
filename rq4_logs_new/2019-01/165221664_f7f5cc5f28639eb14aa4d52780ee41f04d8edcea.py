from html.parser import HTMLParser
from re import sub
from sys import stderr
from traceback import print_exc
from bs4 import BeautifulSoup
import requests
import urllib.request as urllib2



class _DeHTMLParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.__text = []

    def handle_data(self, data):
        #print("Encountered some data  :", data)
        text = data.strip()
        b = self.__text

        if len(b) > 0:
            text = sub('[ \t\r\n]+', ' ', text)
            self.__text.append(text + ' ')

    def handle_starttag(self, tag, attrs):
        #print("Encountered a start tag:", tag)

        # modif moez
        b = self.__text
        if tag == 'style':
            self.clear_cdata_mode()

        if tag == 'script':
            self.clear_cdata_mode()


        c=self.__text
        if tag == 'p':
            self.__text.append('\n\n')
        elif tag == 'br':
            self.__text.append('\n')

        #if tag == 'script':
            #self.__text.append('\n *****SCRIPT****** \n')

    def handle_endtag(self, tag):
        b = self.__text
        #print("Encountered an end tag :", tag)

    def handle_startendtag(self, tag, attrs):
        b = self.__text
        #print("Encountered an end tag :", tag)
        if tag == 'br':
            self.__text.append('\n\n')

        if tag == 'time':
            self.__text.append('\n *****DATE****** \n')


    def text(self):
        b = self.__text
        return ''.join(self.__text).strip()


def dehtml(text):
    try:
        parser = _DeHTMLParser()
        parser.feed(text)
        parser.close()
        return parser.text()
    except:
        print_exc(file=stderr)
        return text
