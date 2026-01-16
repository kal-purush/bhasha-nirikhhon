#!/usr/bin/python
# -*- coding: utf-8 -*-

# Dependencies
import os
import csv
import json
import scraperwiki
import ckanapi
import urllib
import requests
import sys
import hashlib

# Collecting configuration variables
API_KEY = sys.argv[1]
FILE_PATH = sys.argv[2]

# configuring the remote CKAN instance
ckan = ckanapi.RemoteCKAN('http://test-data.hdx.rwlabs.org', apikey=API_KEY)

# This is where the resources are declared. For now,
# they are declared as a Python list.
# This is a skeleton of a function that
# should fetch those schemas using other
# more refined methods.
def getResources(p):
    resources = [
        {
            'resource_id': 'c68fc09f-198c-4978-9fbe-b79dd5ba4658',
            'path': p,
            'schema': {
                "fields": [
                  { "id": "code", "type": "text" },
                  { "id": "title", "type": "text" },
                  { "id": "value", "type": "float" },
                  { "id": "latest_date", "type": "timestamp" },
                  { "id": "source", "type": "text" },
                  { "id": "source_link", "type": "text" },
                  { "id": "notes", "type": "text" },
                  { "id": "explore", "type": "text" },
                  { "id": "units", "type": "text" }
                ]
            },
            'indexes':["code"],
            "primary_key": "code"
        }
    ]
    return resources

# Function to download a resource from CKAN.
def downloadResource(filename, resource_id, apikey):

    # Querying
    url = 'https://data.hdx.rwlabs.org/api/action/resource_show?id=' + resource_id
    headers = { 'Authorization': apikey }
    r = requests.get(url, headers=headers)
    doc = r.json()
    fileUrl = doc["result"]["url"]

    # Downloading
    try:
        r = requests.get(fileUrl, stream=True, headers=headers)
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
    except Exception as e:
        print e
        print 'There was an error downlaoding the file.'

# Function that checks for old SHA hash
# and stores as a SW variable the new hash
# if they differ. If this function returns true,
# then the datastore is created.
def checkHash(filename, first_run, resource_id):
    hasher = hashlib.sha1()
    with open(filename, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
        new_hash = hasher.hexdigest()

    # checking if the files are identical or if
    # they have changed
    if first_run:
        scraperwiki.sqlite.save_var('stag', new_hash)
        new_data = False

    else:
        old_hash = scraperwiki.sqlite.get_var('stag')
        scraperwiki.sqlite.save_var('stag', new_hash)
        new_data = old_hash != new_hash

    # returning a boolean
    return new_data

# 
def updateDatastore(filename, resource_id, resource, apikey):

    # Checking if there is new data
    update_data = checkHash(filename=filename,
                            first_run = False,
                            resource_id=resource_id)
    if (update_data == False):
        print "DataStore Status: No new data. Not updating datastore."
        return

    print "DataStore Status: New data. Updating datastore."

    def upload_data_to_datastore(ckan_resource_id, resource, apikey):
        # Let's delete any existing data before we upload again
        try:
            ckan.action.datastore_delete(resource_id=ckan_resource_id, force=True, apikey=apikey)
        except:
            pass

        ckan.action.datastore_create(
                apikey=apikey,
                resource_id=ckan_resource_id,
                force=True,
                fields=resource['schema']['fields'],
                primary_key=resource['schema'].get('primary_key'),
                indexes=resource['indexes'])

        reader = csv.DictReader(open(resource['path']))
        rows = [ row for row in reader ]
        chunksize = 1000
        offset = 0
        print('Uploading data for file: %s' % resource['path'])
        while offset < len(rows):
            rowset = rows[offset:offset+chunksize]
            ckan.action.datastore_upsert(
                    resource_id=ckan_resource_id,
                    force=True,
                    method='insert',
                    records=rowset)
            offset += chunksize
            complete = str(float(offset)/len(rows) * 100)[:3] + "%"
            print('Update successful: %s completed' % complete)

    # Running the upload function
    upload_data_to_datastore(resource_id, resource, apikey)

    # updating the UI timestamp
    # updateTimestamp(resource_id, apikey)

# Wrapper call for all functions
def runEverything(p):
    # fetch the resources list
    resources = getResources(p)
    print '-------------------------------------'

    # iterating through the provided list of resources
    for i in range(0,len(resources)):
        resource = resources[i]  # getting the right resource
        resource_id = resource['resource_id']  # getting the resource_id
        print "Reading resource id: " + resource_id
        downloadResource(p, resource_id, API_KEY)
        updateDatastore(p, resource_id, resource, API_KEY)
    print '-------------------------------------'
    print 'Done.'
    print '-------------------------------------'


# Error handler for running the entire script
try:
    runEverything(FILE_PATH)
    # if everything ok
    print "ScraperWiki Status: Everything seems to be just fine."
    scraperwiki.status('ok')

except Exception as e:
    print e
    scraperwiki.status('error', 'Creating datastore failed')
    os.system("mail -s 'SNGO Topline: creating datastore failed.' luiscape@gmail.com")
