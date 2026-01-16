import numpy as np
import xml.etree.ElementTree as ET
import re

def get_chem_compounds(section,compound_list):
    

    word_list = section.split(' ')
    for word in word_list:
        
        # Look for character signaling compound
        if '^' in word:
            word = re.sub('[^a-zA-Z0-9-,()^*\/]','',word)
            compound_list.append(word)
        
    return compound_list
        

        
#Read XML file and collect introduction (Background)
def parse_introduction(data,compound_list):

    intro = ''

    #Flag indicating if introduction found
    start = False

    #Search for 'p' section with introduction
    for elem in data:

        #Start of introduction
        if elem.tag == 'heading' and 'background' in elem.text.lower():
            start = True
        
        #Body of introduction
        elif elem.tag == 'p' and start and elem.text != None and re.search('[a-zA-Z0-9]',elem.text):

            for child in elem.iter():
                if (child.tag == 'sub' or child.tag == 'sup') and child.text != None and re.search('[a-zA-Z0-9]',child.text):
                    intro += '^'+child.text+'*'
                elif child.text != None and re.search('[a-zA-Z0-9]',child.text):
                    if child.text[-1] == '.':
                        intro += child.text + ' '
                    else:
                        intro += child.text

                if child.tail != None and re.search('[a-zA-Z0-9]',child.tail):
                    intro += child.tail
        
        #If introduction already found, exit loop
        elif elem.tag == 'heading' and start:
            break

    # Get compounds from intro
    compound_list = get_chem_compounds(intro,compound_list)
    
    return intro, compound_list

def parse_summary(data, compound_list):

    summary = ''

    #Flag indicating if introduction found
    start = False

    #Search for 'p' section with introduction
    for elem in data:

        #Start of introduction
        if elem.tag == 'heading' and 'summary' in elem.text.lower():
            start = True
        
        #Body of introduction
        elif elem.tag == 'p' and start and elem.text != None and re.search('[a-zA-Z0-9]',elem.text):
 
            for child in elem.iter():
                if child.tag == 'sub' and child.text != None and re.search('[a-zA-Z0-9]',child.text):
                    summary += '^'+child.text+'*'
                elif child.text != None and re.search('[a-zA-Z0-9]',child.text):
                    if child.text[-1] == '.':
                        summary += child.text + ' '
                    else:
                        summary += child.text

                if child.tail != None and re.search('[a-zA-Z0-9]',child.tail):
                    summary += child.tail

        #If introduction already found, exit loop
        elif elem.tag == 'heading' and start:
            break

    # Get compounds from summary
    compound_list = get_chem_compounds(summary,compound_list)

    return summary, compound_list

#Read XML file and organizes all citations into two table based on type
def parse_all_citations(citations):
        
    #Arranges data for patcit entries
    def patcit_parse(cit,patcit_table):

        #Indeces for 'patcit' citation table entries
        num_ind = 0
        country_ind = 1
        doc_number_ind = 2
        kind_ind = 3
        name_ind = 4
        date_ind = 5
        category_ind = 6

        #Initialize new row
        cit_row = np.array([0,0,0,0,0,0,0], dtype=object)
        patcit_num = cit[0]
        doc_id = cit[0][0]

        #Get pat number
        cit_row[num_ind] = patcit_num.get('num')

        #Get document id info
        for child in doc_id:
            if child.tag == 'country':
                cit_row[country_ind] = child.text
            elif child.tag == 'doc-number':
                cit_row[doc_number_ind] = child.text
            elif child.tag == 'kind':
                cit_row[kind_ind] = child.text
            elif child.tag == 'name':
                cit_row[name_ind] = child.text
            elif child.tag == 'date':
                cit_row[date_ind] = child.text

        #Get category if available
        cit_row[category_ind] = cit.find('category').text

        #Append citation row
        if patcit_table.size == 0:
            patcit_table = cit_row
        else:        
            patcit_table = np.vstack((patcit_table,cit_row))

        return patcit_table

    #Arranges data for nplcit entries
    def nplcit_parse(cit,nplcit_table):
        #Indeces for 'nplcit' citation table
        num_ind = 0
        othercit_ind = 1
        category_ind = 2

        #Initialize new row
        cit_row = np.array([0,0,0], dtype=object)
        nplcit_num = cit[0]

        #Get pat number
        cit_row[num_ind] = nplcit_num.get('num')

        #Get 'othercit'
        cit_row[othercit_ind] = cit[0][0].text

        #Get cateogry
        cit_row[category_ind] = cit.find('category').text

        #Append citation row
        if nplcit_table.size == 0:
            nplcit_table = cit_row
        else:
            nplcit_table = np.vstack((nplcit_table,cit_row))

        return nplcit_table

    #Gets citations from xml file
    patcit_table = np.empty([0],dtype=object)
    nplcit_table = np.empty([0],dtype=object)

    for cit in citations.iter('us-citation'):

        #Determine citation type to parse accordingle
        cit_type = cit[0].tag

        if cit_type == 'patcit':
            patcit_table = patcit_parse(cit,patcit_table)
        else:
            nplcit_table = nplcit_parse(cit,nplcit_table)
            
    return (patcit_table,nplcit_table)

#Reads XML file and organizes table into dictionary; indexed by table name attribute
def parse_all_tables(data):
    
    #Arranges data for Table entries
    def table_parse(tgroup, num_col, table_name, all_tables):

        current_table = np.empty([num_col], dtype=object)

        #Current table row to fill
        tab_row = np.empty([num_col], dtype=object)
        index = 0

        #Start of the table dat
        tab_start = tgroup.find('tbody')
        
        # read input for each entry and assign to value
        value = ''
        for row in tab_start.findall('row'):
            for entry in row.findall('entry'):
                if entry.text != None and re.search('[a-zA-Z0-9]',entry.text):
                    value = entry.text
                for child in list(entry):
                    if child.tag == 'chemistry':
                        value = child.get('id')
                    if child.text != None and re.search('[a-zA-Z0-9]',child.text):
                        value += child.text
                    if child.tail != None and re.search('[a-zA-Z0-9]',child.tail):
                        value += child.tail
                tab_row[index] = value
                value = ''
                index+=1
            #Append row to table and reset row
            current_table = np.vstack((current_table,tab_row))
            tab_row = np.empty([num_col], dtype=object)
            index = 0

        all_tables[table_name] = current_table
        return all_tables

    #Find tables in file
    all_tables = {}

    #Read through 'p' sections to find tables
    for p in data:

        #see if current 'p' has a table
        if p.find('tables') != None:

            for tables in p.findall('tables'):

                table_name = tables.get('num')
                table = tables.find('table')

                if table != None:

                    for tgroup in table.findall('tgroup'):

                        #Check if tgroup contains data
                        if tgroup.find('tbody') != None:

                            #Number of columns in table
                            num_col = tgroup.get('cols')

                            all_tables = table_parse(tgroup, int(num_col), table_name, all_tables)
    
    return all_tables

#Sets up XML file to be parsed; takes filename as argument
def patent_parse(file_name):
    
    tree = ET.parse(file_name)
    root = tree.getroot()

    #Start of citations
    citations = root.find('us-bibliographic-data-grant').find('us-references-cited')

    #Section containing tables and introduction
    data = root.find('description')

    #List of compounds from intro and summary
    compound_list = []

    #Parse intro and remove citations
    intro, compound_list = parse_introduction(data,compound_list)
    intro = re.sub(r'\s\([a-zA-Z,.\s&]*\s*\d{4}[\)]','',intro)

    #Get summary of invention
    summary, compound_list = parse_summary(data,compound_list)

    patcit_table, nplcit_table = parse_all_citations(citations)

    all_tables = parse_all_tables(data)

    return patcit_table, nplcit_table, all_tables, intro, summary, compound_list


# How to call function
patcit, nplcit, tables, intro, summary, compound_list = patent_parse('C:/Users/cjaik/Documents/vscode/bergen.leon/US09688642-20170627.xml')


print(compound_list)

#look for small element names by subscript element
# get catalog of sections see if there is a way to get other sections