import json
from lxml.etree import fromstring
import re
import requests
import logging
import elasticsearch
from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True


logger = logging.getLogger('ipf_scrape')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

es_url = app.conf["GRQ_ES_URL"]
_index = "grq_v2.0_acquisition-s1-iw_slc"
_type = "acquisition-S1-IW_SLC"
ES = elasticsearch.Elasticsearch(es_url)


def get_manifest(session, info):
    """Get manifest information."""

    # disable extraction of manifest (takes too long); will be
    # extracted when needed during standard product pipeline

    # logger.info("info: {}".format(json.dumps(info, indent=2)))
    manifest_url = "{}/Nodes('{}')/Nodes('manifest.safe')/$value".format(info['met']['alternative'],
                                                                             info['met']['filename'])
    manifest_url2 = manifest_url.replace('/apihub/', '/dhus/')
    for url in (manifest_url2, manifest_url):
        response = session.get(url, verify=False)
        logger.info("url: %s" % response.url)
        if response.status_code == 200:
            break
    response.raise_for_status()
    return response.content


def get_namespaces(xml):
    """Take an xml string and return a dict of namespace prefixes to
       namespaces mapping."""

    nss = {}
    matches = re.findall(r'\s+xmlns:?(\w*?)\s*=\s*[\'"](.*?)[\'"]', xml)
    for match in matches:
        prefix = match[0]; ns = match[1]
        if prefix == '': prefix = '_default'
        nss[prefix] = ns
    return nss


def get_ipf(manifest):
    # append processing version (ipf)
    ns = get_namespaces(manifest)
    x = fromstring(manifest)
    ipf = x.xpath('.//xmlData/safe:processing/safe:facility/safe:software/@version', namespaces=ns)[0]

    return ipf


def get_dataset_json(met, version):
    """Generated HySDS dataset JSON from met JSON."""

    return {
        "version": version,
        "label": met['id'],
        "location": met['location'],
        "starttime": met['sensingStart'],
        "endtime": met['sensingStop'],
    }


def update_ipf(id, ipf_version):
    ES.update(index=_index, doc_type=_type, id=id,
              body={"doc": {"processing_version": ipf_version}})


if __name__ == "__main__":
    '''
    Main program that find IPF version for acquisition
    '''
    ctx = json.loads(open("_context.json","r").read())
    id = ctx["acq_id"]
    met = ctx["acq_met"]

    user = None
    password = None

    # get session
    session = requests.session()
    if None not in (user, password): session.auth = (user, password)

    ds = get_dataset_json(met, version="v2.0")

    info = {
                'met': met,
                'ds': ds,
            }
    manifest = get_manifest(session, info)
    session = requests.session()
    if None not in (user, password): session.auth = (user, password)

    ipf = get_ipf(manifest)
    update_ipf(id, ipf)