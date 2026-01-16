# -*- coding: utf-8 -*-
import jinja2
import ConfigParser
from pprint import pprint

def run_client():
    #./fastd -c etc/fastd.conf -i tap0
    pass

def compute_configurations():

    # Setup Jinja template
    tpl_raw = file('fastd.conf.tpl').read().decode('utf-8')
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(tpl_raw)

    # Read configuration .ini
    config = ConfigParser.ConfigParser()
    config.read('fastd-probe.ini')

    # Get global settings from configuration
    global_settings = {
        'fastd': config.get('fastd-probe', 'fastd'),
        'client': {
            'secret': config.get('client', 'secret'),
            'interface': config.get('client', 'interface'),
            },
        'network': {
            'remote_ip': config.get('network', 'remote_ip'),
            'local_ip': config.get('network', 'local_ip'),
            },
    }

    # Get settings for multiple fastd peers from configuration
    for section in config.sections():
        if section.startswith('peer:'):
            peer_settings = {
                'name': config.get(section, 'name'),
                'key': config.get(section, 'key'),
                'hostname': config.get(section, 'hostname'),
                'port': config.get(section, 'port'),
                }
            settings = global_settings.copy()
            settings.update({'peer': peer_settings})
            #pprint(settings)
            #pprint(settings)
            config = template.render(**settings)
            print config

def run():
    pass

if __name__ == '__main__':
    compute_configurations()
