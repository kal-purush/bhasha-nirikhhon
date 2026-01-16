
# only support lastest python for simplicity

import zipfile, csv, sys, datetime, re
import xml.dom.minidom
import xml.parsers.expat
import argparse

def zipfileopen(zipfile, filename):
    try:
        return zipfile.open(filename)
    except KeyError:
        pass

def parse_workbook(xlsxhandle):
    filehandle = zipfileopen(xlsxhandle, 'xl/workbook.xml')
    workbook_dom = xml.dom.minidom.parse(filehandle)

    workbookpr_dom = workbook_dom.getElementsByTagName('workbookPr')   
    date1904 = len(workbookpr_dom) > 0 and workbookpr_dom[0].getAttribute('date1904') != 'false'

    sheet_dom_list = workbook_dom.getElementsByTagName('sheet')

    sheets = []

    for i, sheet_dom in enumerate(sheet_dom_list):
        name = sheet_dom.getAttribute('name')
        sheets.append({'name': name, 'index': i+1})

    return {'date1904': date1904, 'sheets': sheets}


def parse_styles(xlsxhandle):

    filehandle = zipfileopen(xlsxhandle, 'xl/styles.xml')
    styles_dom = xml.dom.minidom.parse(filehandle)

    numFmt_dom_list = styles_dom.getElementsByTagName('numFmt')
    cellXfs_dom_list = styles_dom.getElementsByTagName('cellXfs')
    xf_dom_list = cellXfs_dom_list[0].getElementsByTagName('xf') 
    
    numFmts = {}
    cellXfs = []
    
    for numFmt_dom in numFmt_dom_list:
        numFmtId = numFmt_dom.getAttribute('numFmtId')
        formatCode = numFmt_dom.getAttribute('formatCode').lower().replace('\\', '')
        numFmts[numFmtId] = formatCode

    for xf_dom in xf_dom_list:
        numFmtId = xf_dom.getAttribute('numFmtId')
        applyNumberFormat = xf_dom.getAttribute('applyNumberFormat')
        xf = numFmtId or applyNumberFormat
        cellXfs.append(xf)

    return {'numFmts': numFmts, 'cellXfs': cellXfs}

class SharedStrings:

    def __init__(self):
        self.in_si = False
        self.in_t = False
        self.strings = []
        self.t_text = ''

    def parse(self, xlsxhandle):
        filehandle = zipfileopen(xlsxhandle, 'xl/sharedStrings.xml')
        if filehandle is None:
            return self.strings
        
        parser = xml.parsers.expat.ParserCreate()
        
        parser.StartElementHandler = self.start_element
        parser.EndElementHandler = self.end_element
        parser.CharacterDataHandler = self.char_data
        parser.ParseFile(filehandle)

        return self.strings

    def start_element(self, name, attrs):
        if name == 'si':
            self.in_si = True
            self.t_text = ''
        elif name == 't':
            self.in_t = True

    def end_element(self, name):
        if name == 'si':
            self.in_si = False
            self.strings.append(self.t_text)
        elif name == 't':
            self.in_t = False

    def char_data(self, data):
        if self.in_t:
            self.t_text += data

def next_col(col):
    t = 0
    for i in col: t = t * 26 + ord(i) - 64
    next = ''
    while t >= 0:
        next = chr(t % 26 + 65) + next
        t = t // 26 - 1
    return next

class Cell:
    def __init__(self, options):
        self.c_r = options['c_r']
        self.t = options['t']
        self.s = options['s']
        self.v_text = options['v_text']

    def __str__(self):
        return str(self.c_r) + ':' + str(self.t) + ':' + str(self.s) + ':' + str(self.v_text)

class Sheet:

    def __init__(self, cell_handler, row_handler):

        self.cell_handler = cell_handler
        self.row_handler = row_handler
        self.in_row = False
        self.in_c = False
        self.in_v = False

        self.v_text = ''
        self.c_r = None
        self.c_s = None
        self.c_t = None
        self.row_r = None
        self.row_spans = None

        self.row = {}

    def parse(self, xlsxhandle):
        filehandle = zipfileopen(xlsxhandle, 'xl/worksheets/sheet1.xml')
        parser = xml.parsers.expat.ParserCreate()
        
        parser.StartElementHandler = self.start_element
        parser.EndElementHandler = self.end_element
        parser.CharacterDataHandler = self.char_data
        parser.ParseFile(filehandle)

    def start_element(self, name, attrs):
        if name == 'row':
            self.in_row = True
            self.row_r = attrs.get('r')
            self.row_spans = attrs.get('span')
            self.row = {}
        elif name == 'c' and self.in_row:
            self.in_c = True
            self.c_r = attrs.get('r')
            self.c_s = attrs.get('s')
            self.c_t = attrs.get('t')
        elif name == 'v' and self.in_c:
            self.in_v = True
            self.v_text = ''

    def end_element(self, name):
        if name == 'v' and self.in_v:
            self.in_v = False
        elif name == 'c' and self.in_c:
            self.in_c = False

            if self.c_r:
                cell = self.cell_handler(Cell({
                    'c_r': self.c_r,
                    's': self.c_s,
                    't': self.c_t,
                    'v_text': self.v_text,
                }))
                self.row[self.c_r] = cell
        elif name == 'row' and self.in_row:
            self.in_row = False
            self.row_handler(self.row)

    def char_data(self, data):
        if self.in_v:
            self.v_text += data

StandardNumFmts = {
    '14': 'mm-dd-yy',
    '16': 'd-mmm',
    '22': 'm/d/yy h:m',
}

FormatTypes = {
    'dd-mmm-yyyy': 'date',
    'yy-mm-dd': 'date',
    'd-mmm-yyyy': 'date',
    'm/d/yy h:m': 'date',
    'd-mmm': 'date',
    'mm-dd-yy': 'date',
    'dd"-"mm"-"yyyy" "hh:mm:ss': 'date',
    'hh:mm:ss': 'time',
    '0.00000': 'float',
}

def is_v_text_a_date(v_text):
    if re.match('^\d+(\.\d+)?$', v_text): return True
    return False

def is_date(v_text, formatCode):
    if re.match('^\d+(\.\d+)?$', v_text):
        if re.match(".*yyyy.*", formatCode): return True

    return False
                
def format_by_numFmtId(cell, numFmtId, numFmts, date1904):
    v_text = cell.v_text
    formatCode = numFmts.get(numFmtId) or StandardNumFmts.get(numFmtId)
    formatType = None
        
    if formatCode is None:
        if is_v_text_a_date(v_text):
            formatType = 'date'
        else:
            print('formatCode not found', numFmtId, v_text)
            return v_text
    else:
        formatType = FormatTypes.get(formatCode)

    if formatType is None:
        if is_date(v_text, formatCode):
            formatType = 'date'
        else:
            print('formatType not found', formatCode, v_text)
            return v_text

    if formatType == 'date':
        date = None
        if date1904:
            date = datetime.datetime(1904, 1, 1) + datetime.timedelta(float(v_text))
        else:
            date = datetime.datetime(1899, 12, 30) + datetime.timedelta(float(v_text))

        return date.strftime('%Y-%m-%d %H:%M:%S')
    
    elif formatType == 'time':
        t = int(round((float(v_text) % 1) * 24 * 60 * 60, 6))
        d = datetime.time(int((t // 3600) % 24), int((t // 60) % 60), int(t % 60))
        return d.strftime('%H:%M:%S')
    
    elif formatType == 'float':
        l = len(formatCode.split('.')[1])
        return ('%.' + str(l) + 'f') % float(v_text)

    return v_text

def xlsx2csv(options):
    xlsxhandle = zipfile.ZipFile(options.xlsxfile)

    workbook = parse_workbook(xlsxhandle)
    styles = parse_styles(xlsxhandle)
    sharedstrings = SharedStrings().parse(xlsxhandle)
    
    def cell_handler(cell):
        if cell.t == 's':
           return sharedstrings[int(cell.v_text)]
        elif cell.t == 'e':
            return ''
        elif cell.s:
            s_int = int(cell.s)
            if s_int < len(styles['cellXfs']):
                numFmtId = styles['cellXfs'][s_int]
                return format_by_numFmtId(cell, numFmtId, styles['numFmts'], workbook['date1904'])

        return cell.v_text

    def row_handler(row):
        print(row)

    sheet = Sheet(cell_handler, row_handler)
    sheet.parse(xlsxhandle)
    
    xlsxhandle.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='xlsx as csv to stdout')
    parser.add_argument('xlsxfile', help='xlsx file path')
    parser.add_argument('--limit', dest='limit', help='rows to write', type=int, default=None)
    parser.add_argument('--sheetname', dest='sheetname', help='sheet name to convert', default=None)
    options = parser.parse_args()

    xlsx2csv(options)