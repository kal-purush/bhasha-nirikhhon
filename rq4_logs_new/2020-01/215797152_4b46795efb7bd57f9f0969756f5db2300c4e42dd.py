#!/usr/bin/env python
#
# Copyright European Organization for Nuclear Research (CERN)

# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#                       http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Panos Paparrigopoulos, <panos.paparrigopoulos@cern.ch>, 2020
#
# Import CRIC user data into Rucio
#
# PY3K COMPATIBLE

import requests
import sys

from rucio.common.exception import RucioException
from rucio.common.types import InternalAccount
from rucio.core.importer import import_accounts
from rucio.db.sqla.constants import AccountStatus, AccountType, IdentityType

CRIC_URLS = {'users': 'http://wlcg-cric-dev-2.cern.ch/api/accounts/user/query/?json',
             'groups': 'http://wlcg-cric-dev-2.cern.ch/api/accounts/group/query/?json'}


def translate_data(cric_users, cric_groups):
    ret = {'accounts': []}
    for user, userdata in cric_users.items():
        for group in userdata['groups']:
            if group['name'] in ['RucioServiceAccounts', 'RucioUserAccounts']:
                account = ({
                    'account': InternalAccount(userdata['username'][:25]),  # FIXME: Shouldn't split 25 first chars of CRIC username.
                    'email': userdata['email'],
                    'account_type': AccountType.SERVICE if group['name'] == 'RucioServiceAccounts' else AccountType.USER,
                    'status': AccountStatus.ACTIVE if userdata['is_active'] else AccountStatus.SUSPENDED,
                    'identities': []
                })
                for profile in userdata['profiles']:
                    if profile['class'] == 'SSLProfile':
                        account['identities'].append({
                            'identity': profile['dn'],
                            'type': 'X509',  # IdentityType.X509,
                            'email': profile['email']
                        })
                ret['accounts'].append(account)

    for group, grpdata in cric_groups.items():
        if grpdata['tag_name'] == 'rucio':
            status = AccountStatus.ACTIVE
            if grpdata['tag_relation'] == 'suspended':
                status = AccountStatus.SUSPENDED
            if grpdata['tag_relation'] == 'deleted':
                status = AccountStatus.DELETED
            account = {
                'account': InternalAccount(group),
                'status': status,
                'account_type': AccountType.GROUP,
                'identities': []
            }
            for user in grpdata['users']:
                for profile in user['sslprofiles']:
                    account['id-entities'].append({
                        'identity': profile['dn'],
                        'type': 'X509',  # IdentityType.X509,
                        'email': profile['email']
                    })
            ret['accounts'].append(account)

    return ret


if __name__ == '__main__':
    users = requests.get(CRIC_URLS.get('users')).json()
    groups = requests.get(CRIC_URLS.get('groups')).json()
    data = translate_data(users, groups)
    try:
        import_accounts(data['accounts'])
        sys.exit(0)
    except RucioException as e:
        print(e)
        sys.exit(2)