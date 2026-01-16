#/usr/bin/env python3

import xml.etree.ElementTree as et
import sqlite3
import requests

xmldoc = requests.get('http://www.fmhdc.com/etc/pollendata/counts.xml').text


root = et.fromstring(xmldoc)

conn = sqlite3.connect('counts.db')
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS
                            counts (date text,
                                    time text, 
                                    location text,
                                    alder float,
                                    willow float,
                                    poplar_aspen float,
                                    birch float,
                                    spruce float,
                                    other1_tree float,
                                    other2_tree float,
                                    total_tree float,
                                    grass float,
                                    grass_2 float,
                                    total_grass float,
                                    weed float,
                                    other1 float,
                                    other2 float,
                                    total_pollen float,
                                    mold float,
                                    comments text)''')

sql_statement_template = '''INSERT INTO counts (%s) VALUES(%s)'''

for child in root.findall('./pollendata'):
    record = {}
    for d in child.findall('./*'):
        try:
            record[d.tag] = float(d.text)
        except:
            record[d.tag] = d.text
    columns = ', '.join(record.keys())
    placeholders = ':' + ', :'.join(record.keys())
    sql = sql_statement_template % (columns, placeholders)
    cur.execute(sql, record)
    conn.commit()