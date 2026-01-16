#!/usr/bin/env python
import os, sys, re, logging, json, requests, argparse
from string import Template

from gcis_clients import GcisClient


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# disable unverified HTTPS warnings
requests.packages.urllib3.disable_warnings()


def update(url):
    """Updated GCIS for this figure."""

    # get authenticated GCIS client
    gcis = GcisClient(url)

    # update figure metadata
    fig_id = "urban-heat-island"
    href = "/report/nca3/chapter/northeast/figure/%s" % fig_id
    update_url = "%s%s" % (url, href)
    data = {
        "identifier": fig_id,
        "report_identifier": "nca3",
        "chapter_identifier": "northeast",
        "title": "Urban Heat Island",
        "url": "http://nca2014.globalchange.gov/highlights/regions/northeast/graphics/urban-heat-island",
        "usage_limits": "Copyright protected. Obtain permission from the original figure source.",
        "caption": "Surface temperatures in New York City on a summer\u2019s day show the \u201curban heat island,\u201d with temperatures in populous urban areas being approximately 10\u00b0F higher than the forested parts of Central Park. Dark blue reflects the colder waters of the Hudson and East Rivers. (Figure source: Center for Climate Systems Research, Columbia University).",
        "source_citation": "Center for Climate Systems Research, Columbia University",
        "ordinal": 5,
        "lat_max": "41.27462",
        "lat_min": "39.38844",
        "lon_max": "-72.00942",
        "lon_min": "-74.65037",
        "time_end": "2003-07-01T15:10:05",
        "time_start": "2003-07-01T15:09:38",
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # update image metadata
    img_id = "f27374a2-d4ef-479c-8f96-9de23fedfc3e"
    href = "/image/%s" % img_id
    update_url = "%s%s" % (url, href)
    data = {
        "identifier": img_id,
        "title": "Urban Heat Island",
        "description": "Image generated from granule: http://earthexplorer.usgs.gov/order/process?node=EC&dataset_name=LANDSAT_TM&ordered=LT50130322003182LGS01",
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # create analyst who generated image and add as contributor
    pers_id = 2472
    href = "/person/"
    update_url = "%s%s" % (url, href)
    check_url = "%s%s" % (update_url, pers_id)
    if requests.get(check_url, verify=False).status_code == 200:
        update_url = check_url
    data = {
        "id": pers_id,
        "first_name": "Matteo",
        "middle_name": None,
        "last_name": "Ottaviani",
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()
    href = "/image/contributors/%s" % img_id
    update_url = "%s%s" % (url, href)
    data = {
        'role': 'analyst',
        'person_id': pers_id,
        'organization_identifier': 'stevens-institute-technology',
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # create source dataset and add contributors
    ds_id = "nasa-landsat-5-tm"
    href = "/dataset/"
    update_url = "%s%s" % (url, href)
    check_url = "%s%s" % (update_url, ds_id)
    if requests.get(check_url, verify=False).status_code == 200:
        update_url = check_url
    data = {
        "identifier": ds_id,
        "description": "LandSat 5 TM",
        "publication_year": 2003,
        "start_time": "1982-11-13T00:00:00",
        "end_time": "2012-05-01T00:00:00",
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()
    href = "/dataset/contributors/%s" % ds_id
    update_url = "%s%s" % (url, href)
    ds_conts = [
        {
            'role': 'data_archive',
            'organization_identifier': 'earth-resources-observation-science-center',
        },
        {
            'role': 'funding_agency',
            'organization_identifier': 'national-aeronautics-space-administration',
        },
        {
            'role': 'distributor',
            'organization_identifier': 'us-geological-survey',
        },
        {
            'role': 'contributor',
            'organization_identifier': 'national-aeronautics-space-administration',
        }
    ]
    for data in ds_conts:
        r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
        r.raise_for_status()

    # create activity that generated image
    act_id = "f27374a2-create-image-urban-heat-island-process"
    href = "/activity/"
    update_url = "%s%s" % (url, href)
    check_url = "%s%s" % (update_url, act_id)
    if requests.get(check_url, verify=False).status_code == 200:
        update_url = check_url
    data = {
        'identifier': act_id,
        'data_usage': "K1 = 666.09 K2 = 1282.71 ; CONVERSION ; To radiance L_lam = G_resc * b6 + B_resc ; To brightness temperature T6 = K2 / alog ( (K1 / L_lam) + 1 )   where b6 is the original band-6 data.",
        'output_artifacts': "/image/f27374a2-d4ef-479c-8f96-9de23fedfc3e",
        'software': "IDL",
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # add derivation prov relating image to activity
    href = "/image/prov/%s" % img_id
    update_url = "%s%s" % (url, href)
    data = {
        'parent_uri': '/dataset/%s' % ds_id,
        'parent_rel': 'prov:wasDerivedFrom',
        'activity': act_id,
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # remove prov relating image to incorrect platform
    href = "/image/prov/%s" % img_id
    update_url = "%s%s" % (url, href)
    data = {
        'delete': {
            'parent_uri': '/platform/landsat-7',
            'parent_rel': 'prov:wasDerivedFrom',
        }
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)

    # add derivation prov relating dataset to platform
    href = "/dataset/prov/%s" % ds_id
    update_url = "%s%s" % (url, href)
    data = {
        'parent_uri': '/platform/landsat-5',
        'parent_rel': 'prov:wasDerivedFrom',
        'activity': act_id,
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()

    # add derivation prov relating dataset to instrument
    href = "/dataset/prov/%s" % ds_id
    update_url = "%s%s" % (url, href)
    data = {
        'parent_uri': '/instrument/thematic-mapper',
        'parent_rel': 'prov:wasDerivedFrom',
    }
    r = gcis.s.post(update_url, data=json.dumps(data), verify=False)
    r.raise_for_status()
   

if __name__ == "__main__":
    desc = "Updated Figure 16.5: Urban Heat Island"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('url', help="GCIS url")
    args = parser.parse_args()
    update(args.url)