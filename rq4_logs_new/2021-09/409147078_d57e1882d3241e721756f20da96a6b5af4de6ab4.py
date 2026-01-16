#!/usr/bin/env python
# coding: utf-8

# In[96]:


import deims


# In[97]:


uuid = "8eda49e9-1f4e-4f3e-b58e-e0bb25dc32a6"
site_record = getSiteById(uuid)


# In[98]:


# pip install rdflib
import os
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, FOAF, ODRL2, ORG, OWL,                            PROF, PROV, RDF, RDFS, SDO, SH, SKOS, SOSA, SSN, TIME,                            VOID, XMLNS, XSD

# relevant attributes from json

# literal has url
# BNode doesn't have a url
# https://www.w3.org/TR/vocab-dcat-3/#Property:resource_identifier

title = BNode(site_record.get('title'))
issued = BNode(site_record.get('created'))
modified = BNode(site_record.get('changed'))
site_url = URIRef("http://www.deims.org/" + uuid)
abstract = Literal(site_record.get('attributes').get('general').get('abstract'))
coordinates = BNode(site_record.get('attributes').get('geographic').get('coordinates'))

# iterable objects
contacts = site_record.get('attributes').get('contact').get('siteManager')
metadataCreators = site_record.get('attributes').get('contact').get('metadataProvider')
observedProperties = site_record.get('attributes').get('focusDesignScale').get('parameters')

g = Graph()

# https://rdflib.readthedocs.io/en/stable/intro_to_creating_rdf.html

g.add((site_url, RDF.type, DCAT.Dataset))
g.add((site_url, DCTERMS['title'], title))
g.add((site_url, DCTERMS["license"], URIRef("https://creativecommons.org/licenses/by-nc/4.0/")))
g.add((site_url, DCTERMS['issued'], issued))
g.add((site_url, DCTERMS['modified'], modified))
g.add((site_url, DCTERMS['language'], Literal("en")))
g.add((site_url, DCTERMS['description'], abstract))
g.add((site_url, DCTERMS['identifier'], URIRef(site_url)))
g.add((site_url, DCAT['landingPage'], URIRef(site_url)))
g.add((site_url, DCTERMS['Location'], coordinates))

# need loops
for contact in contacts:
    g.add((site_url, DCAT['contactPoint'], Literal(contact.get('name'))))
    
for metadata_creator in metadataCreators:
    g.add((site_url, DCAT['creator'], Literal(metadata_creator.get('name'))))
    
for observedProperty in observedProperties:
    g.add((site_url, DCAT['keyword'], Literal(observedProperty.get('label'))))
    g.add((site_url, DCAT['keyword'], URIRef(observedProperty.get('uri'))))

print(g.serialize())

# export to xml

base_dir = os.getcwd() + '/dcat_files'
filename = uuid + '.xml'
destination_path = os.path.join(base_dir, filename)

g.serialize(destination=destination_path, format='xml')

# check validator 
# https://www.itb.ec.europa.eu/shacl/dcat-ap.de/upload


# In[ ]:



