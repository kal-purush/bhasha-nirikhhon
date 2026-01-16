# -*- coding: utf-8 -*-
"""
Fusion Core - Utilities 
=======================

Copyright © 2022 dronectl. All rights reserved.
"""

import json
import yaml
from typing import Any, Dict, Union


def pformat(payload: Any) -> str:
    return "\n" + json.dumps(payload, indent=2)


def parse_yaml(path: str) -> Dict[str, Union[dict, str]]:
    document = {}
    with open(path, 'r') as stream:
        document = yaml.safe_load(stream)
    return document