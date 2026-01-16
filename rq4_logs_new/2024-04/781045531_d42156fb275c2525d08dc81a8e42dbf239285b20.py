from pypdf import PdfReader
from urllib.request import urlopen
import re
import pandas as pd
import requests


reader = PdfReader("pdf_reader/to_read/aus_accredited.pdf")

companies = []
for page_num in range(len(reader.pages)):
    page = reader.pages[page_num]
    tmp = page.extract_text().split('\n')
    if page_num == 0:
        tmp = tmp[2:len(tmp)-2]
        companies += tmp
    elif page_num == len(reader.pages)-1:
        tmp = tmp[5:-11]
        companies += tmp
    else:
        tmp = tmp[5:len(tmp)-2]
        companies += tmp    
           
d = {'Name':companies}
            
clearbit_baseurl = "https://autocomplete.clearbit.com/v1/companies/suggest?query="
urls = []

def query_input(name):
    tmp = name[name.find("(")+1:name.find(")")]
    name = name.replace(" (" + tmp + ")","")
    name = name.replace(" Limited","")
    name = name.replace(" LIMITED","")
    name = name.replace(" LTD","")
    name = name.replace(" Ltd","")
    name = name.replace(" Partnership","")
    name = name.replace(" AUS","")
    return name

for company in d['Name']:
    company = query_input(company)
    url = clearbit_baseurl + company
    response = requests.get(url).json()
    tmp = []
    if response:
        for com in response:
            tmp.append(com['domain'])
    else:
        tmp = [""]
    urls.append(tmp)
      
print(len(urls),len(companies))
d["URL"] = urls
df = pd.DataFrame(d)

df.to_csv('AUS_AE_list.csv')
print("Done!")