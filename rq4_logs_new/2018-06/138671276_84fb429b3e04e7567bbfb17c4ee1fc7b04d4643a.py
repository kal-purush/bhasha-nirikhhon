#!c:\python27\python.exe
# -*- coding: utf-8 -*-

"""

A simple pdf hyper links extractor based on pdfminer.
Parses pdf file and returns all the hyperlinks as an array.
https://github.com/euske/pdfminer/

Technique to check if the input path is URL or local file is adopted from pdfx
https://github.com/metachris/pdfx/blob/master/pdfx/extractor.py

"""

import sys, os.path, re, logging
from pdfminer.psparser import PSKeyword, PSLiteral, LIT
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument, PDFNoOutlines
from pdfminer.pdftypes import PDFObjectNotFound, PDFValueError
from pdfminer.pdftypes import PDFStream, PDFObjRef, resolve1, stream_value
from pdfminer.pdfpage import PDFPage
from pdfminer.utils import isnumber

ESC_PAT = re.compile(r'[\000-\037&<>()"\042\047\134\177-\377]')
def e(s):
    if six.PY3 and isinstance(s,six.binary_type):
        s=str(s,'latin-1')
    return ESC_PAT.sub(lambda m:'&#%d;' % ord(m.group(0)), s)

import six # Python 2+3 compatibility

IS_PY2 = sys.version_info < (3, 0)

if IS_PY2:
    # Python 2
    from cStringIO import StringIO as BytesIO
    from urllib2 import Request, urlopen
else:
    # Python 3
    from io import BytesIO
    from urllib.request import Request, urlopen
    unicode = str

# filterObjs
def filterObjs(obj, x, codec=None):
    if obj is None:
        return
    if isinstance(obj, dict):
        for (k,v) in six.iteritems(obj):
            if(k == "URI"):
                x.append(e(v))
            filterObjs(v, x)
        return

    if isinstance(obj, list):
        return

    if isinstance(obj, (six.string_types, six.binary_type)):
        return

    if isinstance(obj, PDFStream):
        return

    if isinstance(obj, PDFObjRef):
        return

    if isinstance(obj, PSKeyword):
        return

    if isinstance(obj, PSLiteral):
        return

    if isnumber(obj):
        return

    raise TypeError(obj)

#Get all objects in pdf
def getAllObjs( doc, x, codec=None):
    visited = set()
    for xref in doc.xrefs:
        for objid in xref.get_objids():
            if objid in visited: continue
            visited.add(objid)
            try:
                obj = doc.getobj(objid)
                if obj is None: continue
                filterObjs(obj, x, codec=codec)
            except PDFObjectNotFound as e:
                print >>sys.stderr, 'not found: %r' % e
    return

#Parse pdf
def readPdf( stream, x, objids, pagenos, password='',
            dumpall=False, codec=None, extractdir=None):
    try:
        parser = PDFParser(stream)
        doc = PDFDocument(parser, password)
    except Exception as e:
        # print('Caught this error: ' + repr(e))
        raise
    if dumpall:
        getAllObjs(doc, x, codec=codec)
    return x

#Get all the links in the pdf files
def feeder(paths):
    x = []
    links = []
    objids = []
    pagenos = set()
    codec = None
    password = ''
    dumpall = True
    extractdir = None
    fnames = paths

    uri = None
    is_url = False

    stream = None
    reader = None

    URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

    if six.PY2 and sys.stdin.encoding:
        password = password.decode(sys.stdin.encoding)

    for fname in fnames:

        uri = fname

        url = re.findall(URL_REGEX, uri, re.IGNORECASE)
        is_url = len(url)

        if is_url:
            try:
                content = urlopen(Request(uri)).read()
                stream = BytesIO(content)
            except Exception as e:
                # print('Caught this error: ' + repr(e))
                raise
        else:
            try:
                stream = open(uri, "rb")
            except Exception as e:
                # print('Caught this error: ' + repr(e))
                raise

        links = readPdf( stream, x, objids, pagenos, password=password,
            dumpall=dumpall, codec=codec, extractdir=extractdir)
    return links