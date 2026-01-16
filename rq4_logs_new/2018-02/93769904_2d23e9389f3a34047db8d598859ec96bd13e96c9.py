#!/usr/bin/env python
"""
Run `config.yaml.j2` template through Jinja to generate `config.yaml` for use by the application

Usage:
    scripts/generate-config-yaml.py
"""
import os

import jinja2

TEMPLATE_FILE = "config.yaml.j2"
OUTPUT_FILE = "config.yaml"

repo_root_path = os.path.join(os.path.dirname(__file__), "../")
template_path = repo_root_path + TEMPLATE_FILE
output_path = repo_root_path + OUTPUT_FILE

with open(template_path) as f:
    template_content = f.read()

template = jinja2.Template(template_content)
config_yaml = template.render()

with open(output_path, 'w') as f:
    f.write(config_yaml)
os.chmod(output_path, 0o600)