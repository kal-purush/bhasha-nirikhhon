# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import re
import shutil
import tempfile

import pytest
import requests_mock


@pytest.yield_fixture(scope="session", autouse=True)
def tmp_dir():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    tmp = tempfile.mkdtemp(dir=cur_dir)
    tempfile.tempdir = tmp
    yield
    if os.path.isdir(tmp):
        shutil.rmtree(tmp)


@pytest.fixture
def xml():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(cur_dir, 'fixtures/thesis/thesis.xml')


@pytest.fixture
def xml_missing_abstract():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(cur_dir, 'fixtures/thesis_missing_abstract.xml')


@pytest.fixture
def pdf():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(cur_dir, 'fixtures/thesis/thesis.pdf')


@pytest.fixture
def turtle():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(cur_dir, 'fixtures/thesis/thesis.ttl')


@pytest.fixture
def text_errors():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(cur_dir, 'fixtures/text_errors.tab')


@pytest.fixture
def theses_dir():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(cur_dir, 'fixtures')


@pytest.yield_fixture
def fedora():
    with requests_mock.Mocker() as m:
        matcher = re.compile('/rest/tx:123456789/theses/thesis-')
        m.post('/rest/fcr:tx', status_code=201,
               headers={'Location': 'mock://example.com/rest/tx:123456789'})
        m.post('/rest/tx:123456789/fcr:tx/fcr:commit',
               status_code=204)
        m.put('/rest/tx:123456789/theses/thesis',
              status_code=204)
        m.put('/rest/tx:123456789/theses/thesis/thesis.pdf',
              status_code=201)
        m.patch('/rest/tx:123456789/theses/thesis/thesis.pdf/fcr:metadata',
                status_code=204)
        m.put('/rest/tx:123456789/theses/thesis/thesis.txt',
              status_code=201)
        m.patch('/rest/tx:123456789/theses/thesis/thesis.txt/fcr:metadata',
                status_code=204)
        m.patch('/rest/tx:123456789/theses/',
                status_code=204)
        m.patch('/rest/tx:123456789/theses/thesis',
                status_code=204)
        m.put(matcher, status_code=204)
        m.patch(matcher, status_code=204)
        yield m