from PySide2.QtGui import QTextDocument, QGuiApplication
from PySide2.QtPrintSupport import QPrinter
import pandas as pd
import sys
import os


if __name__ == "__main__":
    print('Demo to create images from excel-file')
    test_file = 'C:\\Users\\Klas\\Desktop\\ClubResults_190908.xlsx'
    pdf_file = os.path.splitext(test_file)[0] + '.pdf'
    df = pd.read_excel(test_file, 'Summary')

    df = df[[col for col in df.columns if not col.startswith('Unnamed')]]

    print(df)

    location = 'C:\\Users\\Klas\\Desktop\\ClubResults_190908.html'
    df.to_html(location)

    app = QGuiApplication(sys.argv)

    doc = QTextDocument()
    html = open(location).read()
    doc.setHtml(html)

    printer = QPrinter()
    printer.setOutputFileName(pdf_file)
    printer.setOutputFormat(QPrinter.PdfFormat)
    printer.setPageSize(QPrinter.A4)
    printer.setPageMargins(15, 15, 15, 15, QPrinter.Millimeter)

    doc.print_(printer)
    print("done!")
