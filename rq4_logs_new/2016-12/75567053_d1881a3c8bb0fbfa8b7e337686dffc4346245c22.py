import subprocess
import os

CONFIG_SOPHOS = 'sophos'
CA_TEST = 'REF_Ca'


def write_domains_file(domains, out_file):
    """Creates domain.txt file needed for dehydrated script"""
    with open(out_file, 'w') as output:
        for domain in domains:
            output.write('%s\n'%domains[domain])


def load_domains_file(config_file):
    import ConfigParser

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(config_file)
    domains = {}

    for name, value in config_parser.items(CONFIG_SOPHOS):
        try:
            cert_key = value.split('[')[0]
            if cert_key[:6] == CA_TEST:
                url = raw_input('Enter URL for certificate %s:' % cert_key)
                domains[cert_key] = url
            else:
                continue
        except:
            print("Problem reading config file")
    return domains


def load_domains():
    domains = {}
    p1 = subprocess.Popen("/usr/local/bin/confd-client.plx  get_objects ca host_key_cert".split(),
                          stdout=subprocess.PIPE)
    p2 = subprocess.Popen("grep \'ref\'".split(),
                          stdin=p1.stdout, stdout=subprocess.PIPE)

    for line in p2.communicate():
        if line:
            for segment in line.split(','):
                if segment.split() and segment.split("'")[3][0:6] == 'REF_Ca':
                    domains[segment.split("'")[3]] = raw_input("Full domain for certificate %s: "%segment.split("'")[3])
    return domains


def setup(root):
    import urllib
    import zipfile


    # git isn't installed by default
    urllib.urlretrieve('http://github.com/lukas2511/dehydrated/archive/master.zip', 'master.zip')
    zip = zipfile.ZipFile('master.zip')
    zip.extractall()
    os.remove('master.zip')
    urllib.urlretrieve('http://github.com/lrklomp/sophos-utm-letsencrypt/archive/master.zip', 'master.zip')
    zip = zipfile.ZipFile('master.zip')
    zip.extractall()
    os.remove('master.zip')

    # load sophos cc output
    domains = load_domains()
    # create domains file for dehydrated
    write_domains_file(domains, os.path.join('dehydrated-master', 'domains.txt'))



root_dir = os.getcwd()
certs_path = os.path.join(root_dir, 'dehydrated-master', 'certs')
daily_command = 'dehydrated-master/dehydrated  -k godaddy.py -c'



#setup()
load_domains()

