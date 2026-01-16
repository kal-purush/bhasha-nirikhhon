# rt_jira_csv.py
# Author: afisher
# This script will output the .csv for creating JIRA sub-tasks using the RM scope .csv as input 

import csv

hub = 'Walmart USA' #hub name
doc = '850' #doctype
rt_task = 'RT-12345' #master RT
sch_dt = 'MM/DD/YYYY' #Scheduled Date
owner = 'afisher@spscommerce.com' #Email

infile = open('scope.csv', 'r') #Name your file 'input.csv'
csv_reader = csv.DictReader(infile, dialect='excel')

outfile = open('JIRA_Import_.csv', 'w', newline="")
out_cols = ['Type','Issue ID','Parent ID','Summary','Description','Scheduled','Account Name','RM Coordinator','Project Owner','Summary of Changes','TPID']
csv_writer = csv.DictWriter(outfile, fieldnames=out_cols, dialect='excel')
csv_writer.writeheader()

for row in csv_reader:
    co_name = (row['COMPANY_NAME'])
    tpid = (row['MAX(SP.TPID)'])
    csv_writer.writerow({'Type':'Sub-task',
                         'Parent ID':(rt_task),
                         'Summary':(co_name)+' / '+(hub)+' / '+(doc),
                         'Description':'See Summary of Changes',
                         'Scheduled':(sch_dt),
                         'Account Name':(co_name),
                         'RM Coordinator':(owner),
                         'Project Owner':(owner),
                         'TPID':(tpid)})      

infile.close()
outfile.close()