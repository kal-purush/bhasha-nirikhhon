# -*- coding: utf-8 -*-
from __future__ import absolute_import
import csv
import logging
import os
import xml.etree.ElementTree as ET

import requests


log = logging.getLogger(__name__)
ns = {'mets': 'http://www.loc.gov/METS/',
      'mods': 'http://www.loc.gov/mods/v3'}


class ThesisItem(object):
    '''A thesis object representing a single thesis intellectual entity with
    all its associated metadata.
    '''
    def __init__(self, name, location):
        self.name = name
        self.location = location
        mets = os.path.join(location, name, name + '.xml')
        tree = ET.parse(mets)
        self.root = tree.getroot()
        self.metadata = {
            'abstract': {'pred': 'dcterms:abstract',
                         'obj': self.get_field('abstract', 'abstract')},
            'advisor': {'pred': 'msl:reviewedBy',
                        'obj': self.get_field('advisor',
                                              ('name/*[mods:roleTerm='
                                               '"advisor"]/../mods:namePart')
                                              )},
            'author': {'pred': 'dcterms:creator',
                       'obj': self.get_field('author',
                                             ('name/*[mods:roleTerm='
                                              '"author"]/../mods:namePart'))},
            'copyright': {'pred': 'dcterms:dateCopyrighted',
                          'obj': self.get_field('copyright',
                                                'originInfo/mods:copyrightDate'
                                                )},
            'department': {'pred': 'msl:associatedDepartment',
                           'obj': self.get_field('department',
                                                 ('subject/mods:topic'))},
            'handle': {'pred': 'bibo:handle',
                       'obj': self.get_field('handle',
                                             'identifier[@type="uri"]', 'uri'
                                             )},
            'item_type': {'pred': 'dcterms:type',
                          'obj': 'bibo:Thesis'},
            'publication_date': {'pred': 'dcterms:dateIssued',
                                 'obj': self.get_field('publication_date',
                                                       ('originInfo/'
                                                        'mods:dateIssued'))},
            'publisher': {'pred': 'dcterms:publisher',
                          'obj': '"Massachusetts Institute of Technology"'},
            'rights': {'pred': 'dcterms:rights',
                       'obj': ('"M.I.T. theses are protected by copyright. '
                               'They may be viewed from this source for any '
                               'purpose, but reproduction or distribution in '
                               'any format is prohibited without written '
                               'permission. See provided URL for inquiries '
                               'about permission."')},
            'title': {'pred': 'dcterms:title',
                      'obj': self.get_field('title', 'titleInfo/mods:title')}
            }

    def create_item_turtle_statements(self):
        turtle_file = os.path.join(self.location, self.name,
                                   self.name + '.ttl')
        with open(turtle_file, 'wb') as f:
            f.write(('@prefix bibo: <http://purl.org/ontology/bibo/>\n')
                    .encode('utf-8'))
            f.write(('@prefix dcterms: <http://purl.org/dc/terms/>\n')
                    .encode('utf-8'))
            f.write(('@prefix local: <http://example.com/>\n')
                    .encode('utf-8'))
            f.write(('@prefix msl: <http://purl.org/montana-state/library/>\n')
                    .encode('utf-8'))
            f.write(('@prefix pcdm: <http://pcdm.org/models#>\n')
                    .encode('utf-8'))
            f.write(('\n<>').encode('utf-8'))
            for m in self.metadata:
                f.write(('\n\t' + self.metadata[m]['pred'] + ' ' +
                        self.metadata[m]['obj'] + ' ;').encode('utf-8'))
            f.write(('\n\ta pcdm:Object .').encode('utf-8'))

    def create_file_sparql_update(self, file_ext):
        sparql_file = os.path.join(self.location, self.name,
                                   self.name + file_ext + '.ru')
        with open(sparql_file, 'wb') as f:
            f.write(('PREFIX dcterms: <http://purl.org/dc/terms/>\n')
                    .encode('utf-8'))
            f.write(('PREFIX pcdm: <http://pcdm.org/models#>\n')
                    .encode('utf-8'))
            f.write(('INSERT {').encode('utf-8'))
            f.write(('\n\t<>' + ' dcterms:language ' +
                    self.get_field('language', 'language/mods:languageTerm') +
                    ' ;').encode('utf-8'))
            if file_ext == '.pdf':
                f.write(('\n\t\t' + 'dcterms:extent ' +
                         self.get_field('pages',
                                        'physicalDescription/mods:extent') +
                         ' ;').encode('utf-8'))
            f.write(('\n\t\ta pcdm:File .').encode('utf-8'))
            f.write(('\n} WHERE {\n}').encode('utf-8'))

    def get_field(self, field, search_string, t='string'):
        base = './mets:dmdSec/*/*/*/mods:'
        try:
            result = self.root.find(base + search_string, ns).text
            if t == 'string':
                return '"' + result.replace('"', "'") + '"'
            elif t == 'uri':
                return '<' + result + '>'
        except AttributeError as e:
            log.warning(('Error parsing ' + field +
                         ' field for item ' + self.name))
            return '"None"'

    def add_text_errors(self, text_errors):
        if self.name in text_errors:
            errors = text_errors[self.name]
            if errors['PDFBox err'] != '0':
                self.metadata['no_full_text'] = {'pred': 'local:no_full_text',
                                                 'obj': '"True"'}
            elif errors['No new text'] != '0':
                self.metadata['no_full_text'] = {'pred': 'local:no_full_text',
                                                 'obj': '"True"'}
            elif errors['No old text'] != '0':
                self.metadata['no_full_text'] = {'pred': 'local:no_full_text',
                                                 'obj': '"True"'}
            elif errors['Encoded'] != '0':
                self.metadata['no_full_text'] = {'pred': 'local:no_full_text',
                                                 'obj': '"True"'}
            if errors['Ligatures'] != '0':
                self.metadata['ligatures'] = {'pred': 'local:ligature_errors',
                                              'obj': '"True"'}
            if errors['Line ends'] != '0':
                self.metadata['line_ends'] = {'pred': 'local:line_ends',
                                              'obj': '"True"'}
            if errors['Hex strings'] != '0':
                self.metadata['no_full_text'] = {'pred': 'local:no_full_text',
                                                 'obj': '"True"'}


def parse_text_encoding_errors(tsv_file):
    '''Parse text encoding error log file and return a dict of items with
    errors.
    '''
    with open(tsv_file) as tsv_file:
        text_encoding_errors = {}
        read = csv.DictReader(tsv_file, delimiter='\t')
        for row in read:
            text_encoding_errors[row['Subdir']] = row
        return text_encoding_errors


def create_thesis_item_container(transaction, item, turtle):
    '''Create basic PCDM container for a thesis item.
    '''
    uri = transaction + item
    headers = {'Content-Type': 'text/turtle; charset=utf-8'}
    with open(turtle, 'rb') as payload:
        r = requests.put(uri, headers=headers, data=payload)
    if r.status_code >= 200 and r.status_code < 300:
        log.info(('%s Thesis item created: %s') % (r, r.text))
        return 'Success'
    else:
        log.error(('%s %s ITEM ' + item) % (r, r.text))
        return 'Failure'


def add_thesis_item_file(transaction, item, ext, mimetype, file_path):
    '''Add a file to a thesis item container.
    '''
    uri = transaction + item + '/' + item + ext
    headers = {'Content-Type': mimetype}
    with open(file_path, 'rb') as payload:
        r = requests.put(uri, headers=headers, data=payload)
    if r.status_code >= 200 and r.status_code < 300:
        log.info(('%s Thesis file added: %s') % (r, r.text))
        return 'Success'
    else:
        log.error(('%s %s FILE ' + item + ext) % (r, r.text))
        return 'Failure'


def add_file_metadata(transaction, item, ext, file_path):
    '''Update the metadata for a given file.
    '''
    sparql = file_path + '.ru'
    uri = transaction + item + '/' + item + ext + '/fcr:metadata'
    headers = {'Content-Type': 'application/sparql-update'}
    with open(sparql, 'rb') as data:
        r = requests.patch(uri, headers=headers, data=data)
    if r.status_code >= 200 and r.status_code < 300:
        log.info(('%s File metadata updated: ' + item + ext) % (r))
    else:
        log.error(('%s %s File metadata NOT updated: ' + item + ext)
                  % (r, r.text))


def create_pcdm_relationships(transaction, item):
    '''Create PCDM relationship statements for a thesis item.
    '''
    uri = transaction + item
    headers = {'Content-Type': 'application/sparql-update'}
    data = '''
    PREFIX pcdm: <http://pcdm.org/models#>
    INSERT {
        <> pcdm:hasMember <''' + uri + '''> .
    } WHERE {
    }'''
    r = requests.patch(transaction, headers=headers, data=data)
    log.info(('%s PCDM collection membership created: %s') %
             (r, item))
    pdf = uri + '/' + item + '.pdf'
    text = uri + '/' + item + '.txt'
    data = '''
    PREFIX pcdm: <http://pcdm.org/models#>
    INSERT {
        <> pcdm:hasFile <''' + pdf + '''> .
        <> pcdm:hasFile <''' + text + '''> .
    } WHERE {
    }'''
    r = requests.patch(uri, headers=headers, data=data)
    log.info(('%s PCDM file memberships created: %s') %
             (r, item))


def start_transaction(fedora_uri):
    '''Starts a Fedora transaction.
    Returns location header.
    '''
    uri = fedora_uri + 'fcr:tx'
    r = requests.post(uri)
    log.info('%s Transaction started: %s' % (r, r.headers['Location']))
    return r.headers['Location']


def commit_transaction(location):
    '''Commit and close a Fedora transaction.
    Returns status code.
    '''
    uri = location + '/fcr:tx/fcr:commit'
    r = requests.post(uri)
    if r.status_code == 204:
        log.info('%s Transaction committed.' % (r))
    else:
        log.error('% Transaction failed.' % (r))