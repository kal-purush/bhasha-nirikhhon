# -*- coding: utf-8 -*-

"""

1、安装库
pip install pymupdf
pip install baidu-aip

2、直接运行

"""
import os
import fitz
from tkinter import *
#OCR部分
from aip import AipOcr
def ocrcat(a):
    result=''
    APPID='21876639'
    APIKEY='iqjr21CFPRXhBtWtul5EjMKQ'
    SECRETKEY='ZNYl7ZxXFMl4XyWu4LIse6i7EyGAnBB0'
    c=AipOcr(APPID,APIKEY,SECRETKEY)
    img=open(a,'rb').read()
    message=c.basicGeneral(img)
    file = a.split('.')[0]
    fo = open(file + ".txt", "w+")
    for item in message.get('words_result'):
        fo.write(item['words'])
        fo.write('\n')
        # print(item['words'])

    fo.close()

#拆图片部分

pdf_dir = []

doc_count=0
doc_current=0
def get_file():
    docunames = os.listdir()
    for docuname in docunames:
        if os.path.splitext(docuname)[1] == '.pdf':  # 目录下包含.pdf的文件
            pdf_dir.append(docuname)

def get_file(pdfname):
    pdf_dir.append(pdfname)



def conver_img():
    for pdf in pdf_dir:
        doc = fitz.open(pdf)

        pdf_name = os.path.splitext(pdf)[0]
        index=0
        doc_count=doc.pageCount
        for pg in range(doc.pageCount):
            page = doc[pg]
            rotate = int(0)
            # 每个尺寸的缩放系数为2，这将为我们生成分辨率提高四倍的图像。
            zoom_x = 2.0
            zoom_y = 2.0
            trans = fitz.Matrix(zoom_x, zoom_y).preRotate(rotate)
            pm = page.getPixmap(matrix=trans, alpha=False)
            pm.writePNG('%s%s.png' % (pdf_name,index))
            ocrcat('%s%s.png' % (pdf_name,index))
            path = '%s%s.png' % (pdf_name,index)  # 文件路径
            # print(path)
           # if os.path.exists(path):  # 如果文件存在
                # 删除文件，可使用以下两种方法。
            os.remove(path)
            index+=1
            doc_current=index



if __name__ == '__main__':
    get_file()
    conver_img()