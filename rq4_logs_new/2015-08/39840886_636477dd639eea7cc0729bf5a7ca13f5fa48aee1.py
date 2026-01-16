#!/usr/bin/env python
import os, sys, re, logging, json, requests, argparse
from string import Template

from gcis_clients import GcisClient


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def update(url):
    """Updated GCIS for this figure."""

    # get authenticated GCIS client
    gcis = GcisClient(url)

    # update figure contributor
    href = "/report/nca3/chapter/human-health/figure/contributors/wildfire-smoke-has-widespread-health-effects"
    update_url = "%s%s" % (url, href)
    data = {
        'role': 'convening_lead_author',
        'person_id': 844,
        'organization_identifier': 'columbia-university-mailman-school-public-health', 
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # update image contributor
    img_id = "bf0d948c-6081-4101-bce1-b30542a08d00"
    href = "/image/contributors/%s" % img_id
    update_url = "%s%s" % (url, href)
    data = {
        'role': 'convening_lead_author',
        'person_id': 844,
        'organization_identifier': 'columbia-university-mailman-school-public-health', 
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # create activity that generated image
    act_id = "bf0d948c-adapt-image-wildfire-smoke-widespread-health-effects-process"
    href = "/activity/"
    update_url = "%s%s" % (url, href)
    check_url = "%s%s" % (update_url, act_id)
    if requests.get(check_url, verify=False).status_code == 200:
        update_url = check_url
    data = {
        'identifier': act_id,
        'methodology': "Added star marker for Baltimore, wildfire labels and smoke labels to Postscript image provided by K. Knowlton",
        'output_artifacts': "health_Quebec_wildfires_12788_v5.png",
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # add derivation prov relating to activity
    href = "/image/prov/%s" % img_id
    update_url = "%s%s" % (url, href)
    data = {
        'parent_uri': '/article/10.1021/es035311z',
        'parent_rel': 'prov:wasDerivedFrom',
        'activity': act_id,
        'note': "Image : bf0d948c-6081-4101-bce1-b30542a08d00 was adapted from Figure 3A of the cited article \"Impact of the 2002 Canadian Forest Fires on Particulate Matter Air Quality in Baltimore City\".",
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

   
if __name__ == "__main__":
    desc = "Updated Figure 9.3: Wildfire Smoke has Widespread Health Effects"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('url', help="GCIS url")
    args = parser.parse_args()
    update(args.url)