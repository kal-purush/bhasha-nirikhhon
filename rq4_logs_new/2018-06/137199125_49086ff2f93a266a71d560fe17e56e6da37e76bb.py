#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

def get(event, context): 
    response = {
        'statusCode': 200,
        'body': json.dumps({ 'id':'20001', 'name':'tanaka' })
    }
    return response