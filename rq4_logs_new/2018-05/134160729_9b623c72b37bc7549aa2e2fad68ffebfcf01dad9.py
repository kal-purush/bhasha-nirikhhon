#!/usr/bin/env python
"""DNS Record Updater for DreamHost https://github.com/toddrob99/dh-dns"""

API_URL = "https://api.dreamhost.com/" # You should not need to change this
API_KEY = "" # Generate at https://panel.dreamhost.com/?tree=home.api
DOMAINS = ["dyn.example.com","test.example.com"] # Domains to update, list of strings
COMMENT = "Last updated by dh-dns: {date}" # Comment to add to DNS record, {date} parameter available
UPDATE_INTERVAL = 60 # Minutes
LOG_LEVEL = "INFO" # Options: WARNING, INFO, DEBUG

""" DO NOT CHANGE ANYTHING BELOW THIS LINE """

import logging
import sys
from uuid import uuid4
import urllib
import urllib2
import simplejson as json
from datetime import datetime
from time import sleep
from IPy import IP

class Domain:
    def __init__(self,domain):
        self.name = domain
        self.lastUpdate = None
        logging.info("Domain added: %s",self.name)

class DreamHost:
    cmds = {
            'list': 'dns-list_records',
            'remove': 'dns-remove_record',
            'add': 'dns-add_record'
    }

    def __init__(self,apiUrl,apiKey):
        self.url = apiUrl + "?key=" + API_KEY + "&format=json"
        self.apiKey = apiKey
        self.allDomains = {}

    def api_call(self,cmd):
        url = self.url + "&unique_id="+str(uuid4())[:64]+"&cmd="+cmd
        logging.debug("Making DreamHost API call: %s",url)
        try:
            apiResponse = json.load(urllib2.urlopen(url))
            logging.debug("API Response: %s",apiResponse)
            return apiResponse
        except urllib2.HTTPError, e:
            logging.error("DreamHost API responded with error: %s",e)
            return {'result':'error','data':str(e)}
        except urllib2.URLError:
            logging.error("Connection to DreamHost API failed.")
            return {'result':'error','data':str(e)}
        except Exception, e:
            logging.error("Unknown error encountered while making API call: %s",e)
            return {'result':'error','data':str(e)}

def monitor(domains):
    """ domains is a list of Domain class objects """
    dh = DreamHost(API_URL,API_KEY)
    currentIp = ""

    while True: # Enter update loop
        # Pull current domain info from DH
        dhDomainResponse = dh.api_call(dh.cmds['list']) # List all domains from DH account
        if dhDomainResponse.get('result') == 'success':
            for data in [data for data in dhDomainResponse.get('data') if data.get('type')=='A']:
                dh.allDomains.update({data.get('record'): {'value': data.get('value'), 
                                      'editable': data.get('editable'), 
                                      'comment': data.get('comment')
                                    }})
            logging.debug("All A records from DreamHost: %s",format(dh.allDomains))
        else:
            logging.error("Error reported by DreamHost API: %s.",dhDomainResponse.get('data'))

        newIp = ""

        # Get current IP
        try:
            newIp = urllib2.urlopen("https://toddrob.com/ip.php?bot=1").read().strip()
            ipType = IP(newIp).iptype()
            if ipType != 'PUBLIC':
                logging.warn("%s IP address detected: [%s]. PUBLIC IP required, ignoring.",ipType,newIp)
                newIp = currentIp # Don't want to update to this non-public IP
            else:
                logging.debug("%s IP address detected: [%s].",ipType,newIp)
        except urllib2.HTTPError, e:
            logging.error("IP lookup responded with error: %s",e)
            newIp = currentIp # Don't want to update since there was an error
        except urllib2.URLError:
            logging.error("Connection to IP lookup failed.")
            newIp = currentIp # Don't want to update since there was an error
        except Exception, e:
            logging.error("Unknown error encountered while looking up current IP: %s",e)
            newIp = currentIp # Don't want to update since there was an error

        if newIp == currentIp:
            logging.info("No IP change detected. Current IP: [%s]",currentIp)
        else:
            for domain in domains:
                addFlag = False
                if dh.allDomains.get(domain.name):
                    # Monitored domain already exists
                    if dh.allDomains.get(domain.name).get('editable') == "0":
                        logging.warn("Domain %s is not editable, skipping.",domain.name)
                    elif dh.allDomains.get(domain.name).get('value') == currentIp:
                        logging.info("No update needed for %s.",domain.name)
                    else:
                        # Update needed - remove record and set flag to add it
                        logging.info("New IP detected for %s: [%s]. Deleting existing record.",domain.name,newIp)
                        dhRemoveResponse = dh.api_call(dh.cmds['remove'] + "&type=A&record=" + domain.name + "&value=" + dh.allDomains.get(domain.name).get('value'))
                        if dhRemoveResponse.get('result') == 'success':
                            logging.info("Successfully deleted domain %s.",domain.name)
                            addFlag = True
                        else:
                            logging.error("Error deleting domain %s: %s.",domain.name,dhRemoveResponse.get('data'))
                else:
                    # Monitored domain does not exist
                    logging.info("Domain %s does not exist.",domain.name)
                    addFlag = True
                if addFlag:
                    if len(COMMENT)>0:
                        comment = "&comment=" + urllib.quote_plus(COMMENT.replace("{date}",
                                                                  datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')))
                    else:
                        comment = ""
                    dhAddResponse = dh.api_call(dh.cmds['add'] + "&record=" + domain.name + "&type=A&value=" + newIp + comment)
                    if dhAddResponse.get('result') == 'success':
                        logging.info("Successfully added domain %s with IP [%s].",domain.name,newIp)
                        domain.lastUpdate = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                    else:
                        logging.error("Error adding domain %s: %s.",domain.name,dhAddResponse.get('data'))

        logging.info("Done checking/updating domains. Sleeping for %i minutes.",UPDATE_INTERVAL)
        sleep(UPDATE_INTERVAL*60)

if __name__ == '__main__':
    import os
    cwd = os.path.dirname(os.path.realpath(__file__))
    logLevel = getattr(logging, LOG_LEVEL.upper(),30)
    logging.basicConfig(filename=cwd+"/dh-dns.log", format='%(asctime)s : %(levelname)s : %(message)s', 
                        datefmt='%Y-%m-%d %I:%M:%S %p', level=logLevel)
    logging.info("Logging started with log level %s.",LOG_LEVEL)

    domains = []
    for x in DOMAINS:
        domains.append(Domain(x))

    monitor(domains)