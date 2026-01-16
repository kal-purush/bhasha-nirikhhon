import pandas
import os.path
from Formatos.JSON_DataCollector import JSONDataCollector
from Formatos.HTML_DataCollector import HTMLDataCollector
from Formatos.CSV_DataCollector import CSVDataCollector
from Formatos.XLSX_DataCollector import XLSXDataCollector
from Formatos.XML_DataCollector import XMLDataCollector
import importlib

class DataCollector:
    def __init__(self, files):
        self.singleDataCollector = []
        for file in files:
            extension = os.path.splitext(file)[1]
            format = extension.upper()
            className = format.replace('.','')+'DataCollector'
            formatClassInstance = globals()[className]
            self.singleDataCollector.append((file, formatClassInstance))
        return
           

  
    def readDataSources(self):
        array = []
        for i in self.singleDataCollector:
            object = i[1]
            array.append(object.read(self, i[0]))

        res = pandas.concat(array, ignore_index=True, sort=False)
        return pandas.DataFrame(res)
        return None