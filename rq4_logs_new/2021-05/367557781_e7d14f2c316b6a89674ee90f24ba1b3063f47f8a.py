import os

from PyPDF2 import PdfFileMerger

source_dir = os.getcwd()

merger = PdfFileMerger()

for item in os.listdir(source_dir):
    if item.endswith('pdf'):
        merger.append(item)

merger.write('/Users/christianaguirre/PycharmProjects/untitled/venv/test.pdf')
merger.close()