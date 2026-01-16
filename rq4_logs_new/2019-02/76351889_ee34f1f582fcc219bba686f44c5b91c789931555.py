#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: dang
# 2019-02-12
# Example use: python pdf_split_ocr.py test/TFK.pdf pages_TFK.csv

import os, sys

def page_series_read(filename):
    with open(filename, 'rt') as text_file:
        page_series = text_file.readlines()
    number_series = [int(item) for item in page_series]
    return number_series

def difference_series(number_series):
    differences = [number_series[k+1] - number_series[k] for k in range(len(number_series) - 1)]
    return differences

def pdf_split_series(filename_base, start_pages):
    """
    Split a PDF into small PDF files with starting page of each file in start_pages, use pdftk
    """
    mainname, extname = os.path.splitext(filename_base)  # mainname is '123', extname is '.pdf'
    length_series = difference_series(start_pages)
    print('Start to split the file '+filename_base)
    for k in range(len(length_series)):
        start_page = start_pages[k]
        end_page = start_page + length_series[k] - 1
        newfilename = mainname + '_' + str(start_page) + '-' + str(end_page) + extname
        os.system('pdftk '+filename_base+' cat '+str(start_page)+'-'+str(end_page)+' output '+newfilename)
        print('File ' + newfilename + ' created in the same directory with ' + filename_base)
    print('Finished splitting the file '+filename_base)

def pdf_to_tiff(pdf_file, tiff_file = ''):
    """
    Use ImageMagick to convert PDF to multi-page TIFF, which could be used for OCR
    Note on modifying ImageMagick policy to allow converting PDF: 
    https://stackoverflow.com/questions/42928765/convertnot-authorized-aaaa-error-constitute-c-readimage-453
    In file /etc/ImageMagick-6/policy.xml (or /etc/ImageMagick/policy.xml)
    comment line
    <!-- <policy domain="coder" rights="none" pattern="MVG" /> -->
    change line
    <policy domain="coder" rights="none" pattern="PDF" />
    to
    <policy domain="coder" rights="read|write" pattern="PDF" />
    add line
    <policy domain="coder" rights="read|write" pattern="LABEL" />
    """
    mainname, extname = os.path.splitext(pdf_file)
    if tiff_file == '': tiff_file = mainname + '.tiff'
    os.system('convert -density 300 ' + pdf_file + ' -depth 8 -strip -background white -alpha off ' + tiff_file)
    return tiff_file

def tiff_ocr(tiff_file, text_file = '', **tesseract_args):
    """
    Use the open source software tesseract to OCR a multi-page TIFF file
    Call tesseract with default settings, add language option if available
    """
    mainname, extname = os.path.splitext(tiff_file)
    if text_file == '': text_file = mainname
    tesseract_options = ''
    # parsing tesseract options
    if 'lang' in tesseract_args:
        tesseract_options = ' -1 ' + tesseract_args['lang'] + ' '
    os.system('tesseract ' + tesseract_options + tiff_file + ' ' + text_file) # .txt extension is automatically added by tesseract
    return text_file

def pdf_split_ocr_series(filename_base, start_pages):
    """
    Split a PDF into small PDF files with starting page of each file in start_pages, use pdftk
    Simultaneously use tesseract to extract text from the splitted PDF files (presumably containing images)
    """
    mainname, extname = os.path.splitext(filename_base)  # mainname is '123', extname is '.pdf'
    length_series = difference_series(start_pages)
    print('Start to split the file '+filename_base)
    for k in range(len(length_series)):
        start_page = start_pages[k]
        end_page = start_page + length_series[k] - 1
        newfilename = mainname + '_' + str(start_page) + '-' + str(end_page) + extname
        os.system('pdftk '+filename_base+' cat '+str(start_page)+'-'+str(end_page)+' output '+newfilename)
        print('File ' + newfilename + ' created in the same directory with ' + filename_base)
        tiff_file = pdf_to_tiff(newfilename)
        print('File ' + newfilename + ' converted to ' + tiff_file)
        text_file = tiff_ocr(tiff_file)
        print('File ' + tiff_file + ' was recognized with OCR to ' + text_file)
        # Remove temporary tiff file to save disk space
        os.system('rm ' + tiff_file)
    print('Finished splitting the file ' + filename_base + ' and OCR it to ' + text_file)

if __name__ == "__main__":
    filename_base = str(sys.argv[1])
    file_series = sys.argv[2]
    page_series = page_series_read(file_series)
    pdf_split_ocr_series(filename_base, page_series)