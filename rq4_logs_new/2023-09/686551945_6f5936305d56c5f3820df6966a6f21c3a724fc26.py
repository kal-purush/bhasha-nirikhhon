from fpdf import FPDF
import glob
from common import *

filepaths = glob.glob('data/*.txt')
pdf = FPDF(orientation='P', unit='mm', format='A4')

for f in filepaths:
    header = f.split('/')[-1].split('.')[0]
    pdf.add_page()
    get_header(pdf, header.capitalize())
    with open(f) as file:
        text = ''
        for line in file.readlines():
            text += line
    pdf.set_font(family='Arial', size=8)
    pdf.multi_cell(w=0, h=7, txt=text)
pdf.output(f'output/out.pdf')