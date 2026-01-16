#Given a date or a number, the code will retrieve UN's report about covid19 for that day. 

from datetime import date
from datetime import timedelta

from pathlib import Path
import requests

from SPARQLWrapper import SPARQLWrapper, JSON

def get_covid_report_url(number, by ="number"): 
    
    report_0_date = date(2020, 1, 20)
    requested_report_date = report_0_date + timedelta(days=number)


    base_url = 'https://www.who.int/docs/default-source/coronaviruse/situation-reports/' + requested_report_date.strftime('%Y%m%d')  + "-sitrep-" + str(number) 
    if by == "number":
        
        if number <=7 :
            if number == 6:
                url = base_url + "-2019--ncov.pdf"    
            else:
                url = base_url + "-2019-ncov.pdf"    
        elif number <= 24:
            url = base_url + "-ncov.pdf"    
        else:
            url = base_url + "-covid-19.pdf"
        return(url)     
        

def download_covid_report(number):
    url = get_covid_report_url(number)
    filename = Path('./who_report_covid19_' + str(number) + '.pdf')
    response = requests.get(url)
    filename.write_bytes(response.content)
    




def get_results(endpoint_url, query):
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()


def get_dicitionary_of_outbreaks_to_countries(output_file, print_dict = True ):

    import pandas as pd
    from SPARQLWrapper import SPARQLWrapper, JSON

    endpoint_url = "https://query.wikidata.org/sparql"
    query = """SELECT ?item ?itemLabel ?country ?countryLabel ?alternative
    WHERE 
    {
      ?item wdt:P361 wd:Q83741704.
      ?item wdt:P17 ?country.
      OPTIONAL { ?country skos:altLabel ?alternative . FILTER (lang(?alternative) = "en") }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    """
    
    results = get_results(endpoint_url, query)
    dictionary = pd.json_normalize(results["results"]["bindings"])
    
    dictionary = dictionary[['item.value','itemLabel.value',"country.value", "countryLabel.value", "alternative.value"]]
    dictionary.columns = ["outbreak_qid", "outbreak", "country_qid", "country", "other_names"]

    outbreak_qids = dictionary["outbreak_qid"]
    dictionary["outbreak_qid"] = [item.split("/")[4] for item in outbreak_qids]
    country_qids = dictionary["country_qid"]
    dictionary["country_qid"] = [item.split("/")[4] for item in country_qids]   
    
    if print_dict:
        print(dictionary)

    dictionary.to_csv(output_file)
    
    