'''
machine-learning-sentiment
BUILD DATASET FROM RAW HTML CODES

@ claudio alves monteiro, 2019
'''

arq  = open('data/raw/codes.html', 'r')

html = arq.read()

# clean text


# generate data from html
htcodes = html.split('<a href')
htcodes[2:len(htcodes)][3000]

# create list of only real codes with content
codes = []
for i in htcodes:
    if i[0] == '=' and i[len(i)-1] == '>':
        codes.append(i)


# function to capture unit betwen two characters
def captureInfo(x, init, final):
    code = ''
    for i in range(len(x)):
        # capture infosi
        if x[i] == init:
            cont = i+1
            while x[cont] != final:
                code += x[cont]
                cont += 1
    # treatment
    code = code.replace(' ', '')
    #code = code.replace(':', '-')
    return code

# capture content
def captureContent(unit):
    ''' captures the content of the raw code
    '''
    brs = unit.split('<br>') 
    content = brs[len(brs)-2]
    return content

# capture ata
def captureDoc(unit, doctitle):
    ''' captures the name of the doc
    '''
    for i in range(len(unit)):
        sel = unit[i:i+5]
        if sel == doctitle:
            return unit[i:i+8]
 

def generateData(lista):
    ''' takes a list of raw codes, do a text mining
        process to select the features and 
        returns a dictionary 
    ''' 

    dictdata = {}
    for unit in lista[0:len(lista)]: ## TREAT BETTER
        # capture doc
        doc = captureDoc(unit, 'CEMIT')
        # capture len
        text_range = captureInfo(unit, '[', ']')

        # COTINUE ONLY IF DOC OR TEXT RANGE_RANGE IS.NOT.NULL
        print(doc)
        if doc is not None and text_range is not None:
            ## combine doc and len to create key dictionary
            key = doc+'_'+text_range

            # treatment text_range
            range1 = text_range.split(':')[0]
            range2 = text_range.split(':')[1]
            # capture code  
            code = captureInfo(unit, '#', '+')
            # capture content
            content = captureContent(unit)
            # combine in lista info
            listainfo = [doc, range1, range2, code, content]

            # create dictionary with data
            dictdata[key] = listainfo
    return dictdata

# execute codes and capture data and ids
data = generateData(codes)

import pandas as pd

data = pd.DataFrame(data)
ids = pd.DataFrame(data.columns)

# transpose data
import numpy as np 

adata = np.asarray(data)

npm = adata.transpose()

df = pd.DataFrame(npm)

# combine data with ids
dataset = pd.concat([df, ids], axis=1)

# rename columns
dataset.columns = ['doc', 'init', 'end', 'code', 'content', 'id']

# save dataset
dataset.to_csv('data/interim/datacodes.csv')