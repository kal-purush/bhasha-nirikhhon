import json
import os


def get_comp_conf(stand_conf, key, value):
    name_conf = value[2]
    list_serv = os.popen(key).readlines()
    for str_param in list_serv:
        key_cfg = str_param.strip().split(' ')
        if (len(key_cfg) == 3) and key_cfg[1] == '=':
            key_cfg.remove('=')
        if (len(key_cfg) == 2) and stand_conf is not None:
            if key_cfg[0] in stand_conf[name_conf]:
                if key_cfg[1] != stand_conf[name_conf][key_cfg[0]]:
                    value[0][value[1][0]].append(key_cfg[0])
                    value[0][value[1][1]].append(key_cfg[1])
                    value[0][value[1][2]].append(stand_conf[name_conf][key_cfg[0]])
                    value[0][value[1][3]].append('no pass')
    return value[0]


def rez_comp_conf(file):
    try:
        with open(file, "r") as fh:
            stand_conf = json.load(fh)
    except json.JSONDecodeError:
        stand_conf = None
        print("json.JSONDecodeError")

    sshd_conf = {
            'key_ssh': [],
            'value_ssh': [],
            'value_stand_ssh': [],
            'status_ssh': []
        }

    auditd_conf = {
            'key_audit': [],
            'value_audit': [],
            'value_stand_audit': [],
            'status_audit': []
    }

    service_conf = {
        'sshd -T': [sshd_conf, ['key_ssh', 'value_ssh', 'value_stand_ssh', 'status_ssh'], 'sshd_conf'],
        "cat /etc/audit/auditd.conf": [auditd_conf, ['key_audit', 'value_audit', 'value_stand_audit', 'status_audit'], 'auditd_conf']
    }
    for key, value in service_conf.items():
        yield get_comp_conf(stand_conf, key, value)