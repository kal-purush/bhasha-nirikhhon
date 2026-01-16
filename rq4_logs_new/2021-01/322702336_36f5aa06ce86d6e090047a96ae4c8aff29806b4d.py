import json
from decimal import Decimal
from os import path, environ
from uuid import uuid4

import boto3
import pytest
import responses

import yaml
from moto import mock_dynamodb2

import find_new


def get_table_properties_from_template(resource_name):
    yaml.SafeLoader.add_multi_constructor('!', lambda loader, suffix, node: None)
    template_file = path.join(path.dirname(__file__), '../../cloudformation.yml')
    with open(template_file, 'r') as f:
        template = yaml.safe_load(f)
    table_properties = template['Resources'][resource_name]['Properties']
    return table_properties


@pytest.fixture
def tables():
    with mock_dynamodb2():
        find_new.DB = boto3.resource('dynamodb')

        class Tables:
            subscription_table = find_new.DB.create_table(
                TableName=environ['SUBSCRIPTION_TABLE'],
                **get_table_properties_from_template('SubscriptionTable'),
            )
            product_table = find_new.DB.create_table(
                TableName=environ['PRODUCT_TABLE'],
                **get_table_properties_from_template('ProductTable')
            )

        tables = Tables()
        yield tables


def test_get_active_subscriptions(tables):
    sub1 = {
        'subscription_name': 'test_sub1',
        'geometry': 'POLYGON((-155.826 19.6816,-155.763 19.6816,-155.763 19.7337,-155.826 19.7337,-155.826 19.6816))',
        'file_types': ['SLC'],
        'start': '2020-03-04',
        'end': '2020-03-05',
        'processing_type': 'RTC_GAMMA',
        'processing_parameters': {
        }
    }
    tables.subscription_table.put_item(Item=sub1)

    res = find_new.get_actionable_subscriptions()
    assert res == [sub1]

    sub2 = {
        'subscription_name': 'test_sub2',
        'geometry': 'POLYGON((-155.826 19.6816,-155.763 19.6816,-155.763 19.7337,-155.826 19.7337,-155.826 19.6816))',
        'file_types': ['SLC'],
        'start': '2020-03-04',
        'end': '2020-03-05',
        'processing_type': 'RTC_GAMMA',
        'processing_parameters': {
        }
    }
    sub3 = {
        'subscription_name': 'test_sub3',
        'geometry': 'POLYGON((-155.826 19.6816,-155.763 19.6816,-155.763 19.7337,-155.826 19.7337,-155.826 19.6816))',
        'file_types': ['SLC'],
        'start': '2020-03-04',
        'end': '2020-03-05',
        'processing_type': 'RTC_GAMMA',
        'processing_parameters': {
        }
    }
    tables.subscription_table.put_item(Item=sub2)
    tables.subscription_table.put_item(Item=sub3)

    res = find_new.get_actionable_subscriptions()
    assert res == [sub1, sub2, sub3]


def test_get_existing_products(tables):
    subscription_name1 = 'testsub1'
    subscription_name2 = 'testsub2'
    products = [
        {
            'product_id': str(uuid4()),
            'subscription_name': subscription_name1,
            'hyp3_id': 'hyp3-id-1',
            'status_code': 'PENDING'
        },
        {
            'product_id': str(uuid4()),
            'subscription_name': subscription_name1,
            'hyp3_id': 'hyp3-id-2',
            'status_code': 'RUNNING'
        },
        {
            'product_id': str(uuid4()),
            'subscription_name': subscription_name2,
            'hyp3_id': 'hyp3-id-3',
            'status_code': 'SUCCEEDED'
        },
        {
            'product_id': str(uuid4()),
            'subscription_name': subscription_name2,
            'hyp3_id': 'hyp3-id-4',
            'status_code': 'FAILED'
        },
    ]
    for item in products:
        tables.product_table.put_item(Item=item)

    res = find_new.get_existing_products(subscription_name1)
    assert len(res) == 2
    assert products[0] in res
    assert products[1] in res

    res = find_new.get_existing_products(subscription_name2)

    assert len(res) == 2
    assert products[2] in res
    assert products[3] in res


@responses.activate
def test_get_rtc_granules():
    response_json = {
        'results':
            [
                {'granuleName': 'granule1'},
                {'granuleName': 'granule2'},
                {'granuleName': 'granule3'},
                {'granuleName': 'granule4'},
            ]
    }
    responses.add(responses.GET, find_new.SEARCH_URL, json.dumps(response_json))

    subscription = {
        'geometry': 'UNVALIDATED_WKT',
        'start': 'UNVALIDATED_TIME',
        'end': 'UNVALIDATED_TIME',
        'file_types': 'UNVALIDATED_FILE_TYPE',
    }

    res = find_new.get_rtc_granules(subscription)

    assert res == ['granule1', 'granule2', 'granule3', 'granule4']


@responses.activate
def test_submit_product_to_hyp3():
    subscription = {
        'subscription_name': 'subscription_name1',
        'processing_type': 'RTC_GAMMA',
        'processing_parameters': {
            'dem_matching': False,
            'include_dem': False,
            'include_inc_map': False,
            'include_scattering_area': False,
            'radiometry': 'gamma0',
            'resolution': Decimal(30),
            'scale': 'power',
            'speckle_filter': False,
        }
    }
    granules = ['granule1']

    response_json = {
        'jobs': [
            {
                'job_id': 'foo',
                'job_type': 'RTC_GAMMA',
                'name': subscription['subscription_name'],
                'request_time': '2020-06-04T18:00:03+00:00',
                'status_code': 'PENDING',
                'user_id': 'some_user',

            }
        ],
    }

    responses.add(responses.POST, environ['HYP3_URL'] + '/jobs', json.dumps(response_json))

    res = find_new.submit_product_to_hyp3(subscription, granules)

    assert res.job_id == 'foo'


def test_add_product_for_subscription(tables):
    subscription = {
        'subscription_name': 'subscription_name1',
        'processing_type': 'RTC_GAMMA',
        'processing_parameters': {
            'dem_matching': False,
            'include_dem': False,
            'include_inc_map': False,
            'include_scattering_area': False,
            'radiometry': 'gamma0',
            'resolution': Decimal(30),
            'scale': 'power',
            'speckle_filter': False,
        }
    }
    response_json = {
        'jobs': [
            {
                'job_id': 'foo',
                'job_type': 'RTC_GAMMA',
                'name': subscription['subscription_name'],
                'request_time': '2020-06-04T18:00:03+00:00',
                'user_id': 'some_user',
                'status_code': 'PENDING',
            }
        ],
    }
    granules = ['granule1']
    responses.add(responses.POST, environ['HYP3_URL'] + '/jobs', json.dumps(response_json))

    find_new.add_product_for_subscription(subscription, granules)

    table_contents = tables.product_table.scan()['Items']

    assert len(table_contents) == 1
    assert table_contents[0]['hyp3_id'] == 'foo'
    assert table_contents[0]['granules'] == granules


@responses.activate
def test_lambda_handler(tables):
    subscriptions = [
        {
            'subscription_name': 'subscription_name1',
            'geometry': 'UNVALIDATED WKT',
            'processing_type': 'RTC_GAMMA',
            'processing_parameters': {
                'dem_matching': False,
                'include_dem': False,
                'include_inc_map': False,
                'include_scattering_area': False,
                'radiometry': 'gamma0',
                'resolution': 30,
                'scale': 'power',
                'speckle_filter': False,
            },
            'start': '2020-03-04',
            'end': '2020-03-05',
            'file_types': ['SLC'],
        },
    ]

    for item in subscriptions:
        tables.subscription_table.put_item(Item=item)

    search_json = {
        'results':
            [
                {'granuleName': 'granule1'},
                {'granuleName': 'granule2'},
                {'granuleName': 'granule3'},
                {'granuleName': 'granule4'},
            ]
    }

    hyp3_json = {
        'jobs': [
            {
                'job_id': 'foo',
                'job_type': 'RTC_GAMMA',
                'name': 'subscription_name1',
                'request_time': '2020-06-04T18:00:03+00:00',
                'user_id': 'some_user',
                'status_code': 'PENDING',
            }
        ],
    }

    responses.add(responses.GET, find_new.SEARCH_URL, json.dumps(search_json))
    responses.add(responses.POST, environ['HYP3_URL'] + '/jobs', json.dumps(hyp3_json))

    find_new.lambda_handler(None, None)

    products = tables.product_table.scan()['Items']
    assert len(products) == 4
    assert [granule['granules'][0] for granule in products] == ['granule1', 'granule2', 'granule3', 'granule4']