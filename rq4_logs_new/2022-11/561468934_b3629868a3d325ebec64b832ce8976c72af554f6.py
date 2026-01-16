#!/usr/bin/env python3

# Usage:
# Set TFE_TOKEN environment variable
# Supply URL if not using https://app.terraform.io
# ./tfe_resources.py --url https://terraform.companyname.com --output file.csv

import requests
import argparse
import os
import csv

parser = argparse.ArgumentParser(description='Parse arguments')
parser.add_argument('--url', help='Parse TFE URL, leave blank for TFC',
                    default='https://app.terraform.io', required=False)
parser.add_argument('--output', help='Output file name', required=True)

args = parser.parse_args()

# Get TFE_TOKEN environment variable
token = os.environ.get('TFE_TOKEN')
headers = {'Authorization': 'Bearer ' + token,
           'Content-Type': 'application/vnd.api+json'}

if token is None:
    print('Environment variable TFE_TOKEN not set')
    exit(1)


class organizations:
    def __init__(self, id, created_at):
        self.id = id
        self.created_at = created_at


class workspaces:
    def __init__(self, id, name, org, date_created, date_changed):
        self.id = id
        self.name = name
        self.org = org
        self.date_created = date_created
        self.date_changed = date_changed


class resources:
    def __init__(self, name, type, identifier, org, workspace_id, workspace_name):
        self.name = name
        self.type = type
        self.identifier = identifier
        self.org = org
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name


def get_organizations():
    list = []
    url = args.url + '/api/v2/organizations'
    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        for org in r.json()['data']:
            list.append(organizations(
                org['id'], org['attributes']['created-at']))

    else:
        print('Error ({}) getting organizations'.format(r.status_code))
        exit(1)

    return list


def get_workspaces(orgs):
    """
    GET /organizations/:organization_name/workspaces
    https://www.terraform.io/docs/cloud/api/workspaces.html#list-workspaces
    """
    list = []

    for org in orgs:
        url = args.url + '/api/v2/organizations/{}/workspaces'.format(org.id)
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            for workspace in r.json()['data']:
                list.append(workspaces(workspace['id'],
                                       workspace['attributes']['name'],
                                       org.id,
                                       workspace['attributes']['created-at'],
                                       workspace['attributes']['updated-at']))
        else:
            print('Error ({}) getting workspaces for "{}" organization'.format(
                r.status_code, org.id))
            exit(1)
    return list


def get_current_state(ws):
    """
    Download state file for workspace and parse resources
    GET /workspaces/:workspace_id/current-state-version
    https://www.terraform.io/docs/cloud/api/state-versions.html#show-a-state-version
    """

    list = []

    for workspace in ws:
        url = args.url + \
            '/api/v2/workspaces/{}/current-state-version'.format(workspace.id)
        r = requests.get(url, headers=headers)

        if (r.status_code == 200):
            state_url = r.json()[
                'data']['attributes']['hosted-state-download-url']
        else:
            print('Error ({}) getting state for "{}" workspace'.format(
                r.status_code, workspace.name))
            exit(1)

        r = requests.get(state_url, headers=headers)
        if (r.status_code == 200):
            for resource in r.json()['resources']:
                name = resource['name']
                type = resource['type']
                for instance in resource['instances']:
                    if "arn" in instance['attributes']:
                        identifier = instance['attributes']['arn']
                    else:
                        identifier = None

                    list.append(resources(name, type, identifier,
                                workspace.org, workspace.id, workspace.name))

    return list


orgs = get_organizations()
ws = get_workspaces(orgs)
res = get_current_state(ws)

fields = ['name', 'type', 'identifier',
          'org', 'workspace_id', 'workspace_name']

with open(args.output, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()
    for resource in res:
        writer.writerow({'name': resource.name,
                         'type': resource.type,
                         'identifier': resource.identifier,
                         'org': resource.org,
                         'workspace_id': resource.workspace_id,
                         'workspace_name': resource.workspace_name})