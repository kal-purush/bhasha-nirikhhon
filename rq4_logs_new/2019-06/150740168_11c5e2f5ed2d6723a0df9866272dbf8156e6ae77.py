import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import ssl
import pandas as pd
import time
from pprint import pprint as pp
import json
print("Done")

#Initialize variables for Lymphoma data scraping
columns = ['PII','Title', 'Case']
df_others = pd.DataFrame(columns=columns)
numArticle = 1

diseases = [ "histoplasmosis", "sarcoidosis", "cat+scratch", "fungal+infection", "nontuberculous+mycobacteria", "bacterial+adenitis" , "aspergillosis", "cystic+fibrosis", "aspiration+pneumonitis", "laryngotracheal+papillomatosis" ]
testbody = None
for disease in diseases:
    print("Disease:", disease)
    for count in range(0,5000,25):
        url = 'https://api.elsevier.com/content/search/sciencedirect?'
        apikey = '7f59af901d2d86f78a1fd60c1bf9426a'
        query = disease + '+case+report'
        url = url + 'query='+ query +'&apiKey=' + apikey + '&start=' + str(count)

        with urllib.request.urlopen(url) as response:
            result = json.loads(response.read().decode("utf-8"))
            for i in range(len(result['search-results']['entry'])):
                pii=result['search-results']['entry'][i]['pii']
                print(pii)
                title=result['search-results']['entry'][i]["dc:title"]
                u="https://api.elsevier.com/content/article/pii/"+pii+"?apiKey=444438d929e036b98383ca350673128c"
                print(u)
                if (title.lower().find('case')!=-1 or title.lower().find('manifesting')!=-1 or title.lower().find('manifestation')!=-1 or title.lower().find('presenting')!=-1 or title.lower().find('presentation')!=-1) and title.lower().find(disease)!=-1: #if title contains "case"
                    #open article URL
                    
                    h = urllib.request.urlopen(u).read()
                    s = BeautifulSoup(h)
                    body=s.find("body")

                    if body:
                        sections=body.find("ce:sections")
                        #print(case)
                        casefound=False
                        if (sections):
                            section=sections.find("ce:section")
                            while (section):
                                sectiontitle = section.find("ce:section-title")                            
                                if (sectiontitle.text.lower().startswith("observ") or sectiontitle.text.lower().startswith("present") or sectiontitle.text.lower().startswith("case") or sectiontitle.text.lower().startswith("report")):
                                    print(sectiontitle.parent.text[0:100])
                                    articleInfo=[pii, title, sectiontitle.parent.text.replace('\n', ' ')]
                                    df_others.loc[numArticle-2] = articleInfo
                                    casefound=True
                                    break;
                                section = section.find_next_sibling("ce:section")
                            numArticle=numArticle+1
                    print(" ")
                    print("-----------------------------------------------")

                    if not casefound:       
                        print(title)
                        print(numArticle, "\nNo case section\n")
                else:
                    print("No access:", u)
                
        if numArticle % 50 == 0:
            break;
print("Done")
df_others.head()