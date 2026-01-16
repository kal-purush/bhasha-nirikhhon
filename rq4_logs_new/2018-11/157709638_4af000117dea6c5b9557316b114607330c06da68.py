#!/usr/bin/env python3

import json
import requests
from requests.auth import HTTPBasicAuth
import urllib3
import argparse
import os
import base64
import logging
import time
urllib3.disable_warnings()


headers = {'content-type': "application/json"}

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('imx_mon_{}.log'.format(time.strftime("%Y-%m-%d_%H%M%S")))
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def args_from_cfgfile(file):
    with open('{}/{}'.format(scriptpath, file)) as cfg:
        conf_file = dict(line for line in (l.split() for l in cfg) if line)
    return conf_file


def get_ibox_sn():
    return ibx.get('{}system'.format(iboxurl)).json()['result']['serial_number']


def get_qos_entities():
    outp = ibx.get("{}qos/assigned_entities?page_size=1000".format(iboxurl))
    qoslist = []
    for page in range(1, outp.json()['metadata']['pages_total']+1):
        qosout=ibx.get(url='{}qos/assigned_entities?page_size=1000&page={}'.format(iboxurl, page))
        for item in qosout.json()['result']:
            qoslist.append(item)
        return qoslist


def get_bfactor(qoslist):
    policyid = set()
    policylist = []
    for item in qoslist:
        policyid.add(item['qos_policy_id'])
    for p in policyid:
        policylist.append(ibx.get(url='{}qos/policies/{}'.format(iboxurl, p)).json()['result'])
    return policylist
    

def get_imx_vol(vol):
    imxvol=imx.get('{}?name={}'.format(imxurl, vol))
    if imxvol.json()['result']:
        return imxvol.json()['result'][0]
    else:
        logger.error("volume not found in infinimetrics")


def get_vol_iops(vol):
    imxvol=get_imx_vol(vol)
    outp=imx.get('{}?sort=-timestamp'.format(imxvol['data']))
    if outp.json()['result']:
        latest=outp.json()['result'][0]
        policy=latest['max_ops']
        policyname = [p['name'] for p in policylist if p['id'] == [i['qos_policy_id'] for i in qos_ent if i['entity_id'] == imxvol['id_in_system']][0]]
        if (policy) and (latest['read_ops']+latest['write_ops'] > policy):
            bf = [p['burst_factor'] for p in policylist if p['id'] == [i['qos_policy_id'] for i in qos_ent if i['entity_id'] == imxvol['id_in_system']][0]]
            if bf[0]:
                policy = bf[0]*latest['max_ops']
        if (policy) and (latest['read_ops']+latest['write_ops'] > policy*float(warning_threshold)):
            if (policy) and (latest['read_ops']+latest['write_ops'] > policy*float(alert_threshold)):
                create_event('Volume {} reached {} IOPS and has exceeded {}% of its QoS policy: {} '.format(imxvol['name'], latest['read_ops']+latest['write_ops'], int(100*float(alert_threshold)) ,policyname[0]), 'WARNING')
            else:
                create_event('Volume {} reached {} IOPS and has exceeded {}% of its QoS policy: {} '.format(imxvol['name'], latest['read_ops']+latest['write_ops'], int(100*float(warning_threshold)) ,policyname[0]), 'INFO')
    else:
        logger.error('no data found')
            

def create_event(msg, level):
    evnt={
    "description_template": "{}".format(msg), 
    "data": None, 
    "visibility": "CUSTOMER", 
    "level": level
    }
    ibx.post('{}events/custom'.format(iboxurl), json=evnt)

    
def get_args():
    """
    Supports the command-line arguments listed below.
    """
    parser = argparse.ArgumentParser(description="Script for managing snap groups.")
    parser.add_argument('-c', '--credfile', nargs=1, required=True, help='Credentials file name')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    if args.credfile:
        scriptpath = os.path.dirname(os.path.abspath(__file__))
        if os.path.isfile('{}/{}'.format(scriptpath, args.credfile[0])):
            ibx = requests.session()
            imx = requests.session()
            cfgargs = args_from_cfgfile(args.credfile[0])
            ibox1=cfgargs['ibox']
            imax=cfgargs['infinimetrics']
            user=cfgargs['user']
            enc_pw=cfgargs['password']
            warning_threshold=cfgargs['warning']
            alert_threshold=cfgargs['alert']
            pw = base64.b64decode(enc_pw).decode("utf-8", "ignore")
            creds1 = HTTPBasicAuth(user, pw)
            iboxurl = "http://{}/api/rest/".format(ibox1)
            ibx.auth = creds1
            sn=get_ibox_sn()
            imxurl = "http://{}/api/rest/systems/{}/monitored_entities/".format(imax, sn)
            imx.auth = creds1
            imx.verify = False
            ibx.headers.update = headers
            qos_ent = get_qos_entities()
            policylist = get_bfactor(qos_ent)
            for vol in qos_ent:
                if vol['entity_type'] == 'VOLUME':
                    get_vol_iops(vol['entity_name'])
        else:
            logger.error('Credentials File Not Found')