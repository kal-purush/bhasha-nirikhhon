#!/usr/bin/python

import re

def firewalld_zone_info(info_str):
    info_lines = info_str.splitlines()
    zone_info = {
        'name': info_lines[0].split(' ')[0],
        'active': info_lines[0].endswith(' (active)'),
        'rich_rules': []
    }
    i = 0
    while i + 1 < len(info_lines):
        i += 1
        line = info_lines[i]
        m = re.match('  (\S+|rich rules):\s*(.*)', line)
        if m == None:
            raise Exception("Unable to parse line " + line)
        key = m.group(1)
        value = m.group(2)
        r_key = key.replace('-','_')
        if key == 'rich rules':
            while i + 1 < len(info_lines) and info_lines[i + 1].startswith("\t"):
                i += 1
                # FIXME
                pass
        elif key in (
            'interfaces',
            'sources',
            'services',
            'ports',
            'protocols',
            'forward-ports',
            'source_ports',
            'icmp-blocks'
        ):
            if value == '':
                zone_info[r_key] = []
            else:
                zone_info[r_key] = value.split(' ')
        else:
            zone_info[r_key] = value
    return zone_info

class FilterModule(object):
    '''
    custom jinja2 filter for parsing output of firewall-cmd --info-zone
    '''

    def filters(self):
        return {
            'firewalld_zone_info': firewalld_zone_info
        }
