# -*- coding: utf-8 -*-
{
    "name": 'Chilean DPA',
    'category': 'Localization/Chile',
    "summary": 'Chilean administrative division',
    "author": 'maitaoriana',
    "version": '1.0',
    "depends": ['base_address_city'],
    "license": "LGPL-3",
    "data": [
        'security/ir.model.access.csv',
        'data/res_country_state_region.xml',
        'data/res_country_state.xml',
        'data/res_city.xml',
        'data/res_country_data.xml',
        'views/res_state_view.xml',
    ],
    "installable": True,
    "application": False,
    'auto_install': False,
}