import os
import nbformat as nbf
import json
import string
from nbformat.v4.nbbase import (
    new_code_cell, new_markdown_cell, new_notebook,
    new_output, new_raw_cell
)

data_dictionary = json.load(open('data_dictionary.json'))
data_files = os.listdir('./Data')

cells = []
cells.append(new_code_cell(source="import pandas as pd\nimport matplotlib.pyplot as plt\nfrom dp_lib import dateline, catbar, numstats, missingness, text_search, check_dups, flow_stats, lab_stats"))

for table in data_dictionary:
    csv_file = string.capwords(table['element'].replace('_', ' ')).replace(' ', '_') + '.csv'
    if csv_file in data_files:
        cells.append(new_markdown_cell(source=f"# {table['element']}"))
        table_df = f"{table['element'].lower()}_df"
        cells.append(new_code_cell(source=f"{table_df} = pd.read_csv('Data/{csv_file}')\n{table_df}"))
        cells.append(new_code_cell(source=f"check_dups({table_df})"))
        cells.append(new_code_cell(source=f"missingness({table_df})"))
        if table['date_field']:
            cells.append(new_code_cell(source=f"dateline({table_df}, '{table['date_field']}')"))

        f = open('Data/' + csv_file, encoding='utf-8')
        fields = [field.replace('"','').strip() for field in f.readline().split(',')]
        for nb_field in [f for f in table['fields'] if f['nb_func']]:
            if nb_field['field_name'] in fields:
                if nb_field['nb_func'] == 'catbar':
                    cells.append(new_code_cell(source=f"{nb_field['nb_func']}({table_df}, \'{nb_field['field_name']}\', graph=False) ## Set graph=True for Bar graph"))
                else:
                    cells.append(new_code_cell(source=f"{nb_field['nb_func']}({table_df}, \'{nb_field['field_name']}\')"))

                if table_df == 'flowsheet_vitals_df':
                    cells.append(new_code_cell(source=f"flow_stats({table_df})"))
                elif table_df == 'labs_df':
                    cells.append(new_code_cell(source=f"lab_stats({table_df}, top=10)"))

nb = new_notebook(cells=cells, metadata={ 'language' : 'python' })
nbf.write(nb, open('Data_Profiler.ipynb', 'w', encoding='utf-8'), 4)
