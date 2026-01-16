# Test Parser
# http://stackoverflow.com/questions/29155628/python-minidom-parse-xml-file-and-write-to-csv

from xml.dom.minidom import parse
import csv
from sys import getsizeof

def writeToCSV(myLibrary):
    csvfile = open('output.csv', 'wb')
    fieldnames = ['title', 'creator', 'type' , 'NIItype' , 'format' , 'URI', 'fullTextURL' ,'language', 'alternative' ,'dateofissued']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, lineterminator="\n", delimiter='\t')
    writer.writeheader()

    books = myLibrary.getElementsByTagName("meta")

    for book in books:
        titleValue = book.getElementsByTagName("title")[0].childNodes[0].data
        typeValue = book.getElementsByTagName("type")[0].childNodes[0].data
        NIItypeValue = book.getElementsByTagName("NIItype")[0].childNodes[0].data
        formatValue = book.getElementsByTagName("format")[0].childNodes[0].data
        URIValue = book.getElementsByTagName("URI")[0].childNodes[0].data
        fullTextURLValue = book.getElementsByTagName("fullTextURL")[0].childNodes[0].data
        languageValue = book.getElementsByTagName("language")[0].childNodes[0].data
        dateofissuedValue = book.getElementsByTagName("dateofissued")[0].childNodes[0].data if book.getElementsByTagName("dateofissued") else ""
        alternativeValue = book.getElementsByTagName("alternative")[0].childNodes[0].data if book.getElementsByTagName("alternative") else ""
        counta = 0
        for author in book.getElementsByTagName("creator"):
            # authorValue = author.childNodes[0].data
            counta += 1
            if counta > 1:
                authorValue = authorValue + '/' + author.childNodes[0].data
            else:
                authorValue = author.childNodes[0].data
        writer.writerow({'title': titleValue.encode("utf-8"), 'creator': authorValue.encode("utf-8"), 'type':typeValue.encode("utf-8"), 'NIItype':NIItypeValue.encode("utf-8"), 'format': formatValue, 'URI': URIValue , 'fullTextURL':fullTextURLValue, 'language':languageValue, 'alternative':alternativeValue.encode("utf-8") ,'dateofissued': dateofissuedValue})

doc = parse('request.xml')
myLibrary = doc.getElementsByTagName("ListRecords")[0]

# Get book elements in library
books = myLibrary.getElementsByTagName("meta")

# Print each book's title
writeToCSV(myLibrary)