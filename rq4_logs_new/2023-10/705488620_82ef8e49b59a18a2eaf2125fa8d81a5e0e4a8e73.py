#!/usr/bin/env python3

import os
import subprocess
import shutil

# Clone the repository
# subprocess.run(
#     ['git', 'clone', 'https://github.com/zaffron/public_ansible_homelab.git'])
# os.chdir('public_ansible_homelab')

# Read user input
username = input("Enter username: ")
puid = input("Enter puid of the user: ")
pgid = input("Enter pgid of the user: ")
server_ip = input("Enter server IP address: ")
domain_name = input("Enter the domain name: ")
timezone = input("Enter timezone (e.g. asian/bangkok): ")
timezone_title = input("Enter timezone in title case (e.g. Asia/Bangkok) because some service are demanding: ")

# pihole
pihole_interface = input("Enter pihole interface(eth0 was in my case): ")
pihole_web_password = input("Enter password for pihole web: ")

# wireguard
wg_password = input("Enter password for wireguard: ")

# cloudflare
cloudflare_email = input("Enter the Cloudflare email for traefik: ")
cloudflare_dns_api_token = input("Enter the Cloudflare DNS API token for traefik: ")
cloudflare_zone_api_token = input("Enter the Cloudflare Zone API token for traefik: ")

# ssh
use_password = input("Are you using a password instead of SSH keys? [y/n]: ")

# traefik
traefik_user_hash = input("Enter traefik dashboard user hash: ")

# authelia
jwt_secret = input("Enter jwt secret for authelia: ")
encryption_key = input("Enter encryption key for authelia sqlite database: ")
gmail_address = input("Enter gmail address for authelia smtp: ")
gmail_password = input("Enter gmail insecure app password for authelia smtp: ")
admin_email = input("Enter email for authelia admin: ")
admin_password = input("Enter argon2id password hash for authelia admin: ")

# photoprism
photoprism_mysql_user = input("Enter mysql username for photoprism: ")
photoprism_mysql_db = input("Enter mysql database name for photoprism: ")
photoprism_mysql_root_password = input("Enter mysql root password for photoprism: ")
photoprism_mysql_password = input("Enter mysql password for photoprism: ")
photoprism_admin_password = input("Enter admin password for photoprism: ")

# plex
plex_claim = input("Enter plex claim token: ")

# Replace values in vars.yml file
with open('group_vars/all/vars.yml', 'r') as f:
    content = f.read()
content = content.replace('<username>', username)
content = content.replace('<puid>', puid)
content = content.replace('<pgid>', pgid)
content = content.replace('<server_ip>', server_ip)
content = content.replace('<domain_name>', domain_name)
content = content.replace('<timezone>', timezone)
content = content.replace('<timezone_title_case>', timezone_title)

content = content.replace('<pihole_interface>', pihole_interface)
content = content.replace('<pihole_web_password>', pihole_web_password)

content = content.replace('<wg_pass>', wg_password)

content = content.replace('<cloudflare_email>', cloudflare_email)
content = content.replace('<cloudflare_dns_api_token>', cloudflare_dns_api_token)
content = content.replace('<cloudflare_zone_api_token>', cloudflare_zone_api_token)

content = content.replace('<traefik_basic_auth_hash>', traefik_user_hash)

content = content.replace('<authelia_sqlite_encryption_key>', encryption_key)
content = content.replace('<google_mail>', gmail_address)
content = content.replace('<google_insecure_app_pass>', gmail_password)
content = content.replace('<authelia_admin_mail>', admin_email)
content = content.replace('<authelia_admin_argon2id_pass>', admin_password)
content = content.replace('<jwt_secret>', jwt_secret)

content = content.replace('<photoprism_mysql_user>', photoprism_mysql_user)
content = content.replace('<photoprism_mysql_db>', photoprism_mysql_db)
content = content.replace('<photoprism_mysql_root_password>', photoprism_mysql_root_password)
content = content.replace('<photoprism_mysql_password>', photoprism_mysql_password)
content = content.replace('<photoprism_admin_password>', photoprism_admin_password)

content = content.replace('<plex_claim>', plex_claim)

source_file = 'vars_example.yml'
destination_file = 'group_vars/all/vars.yml'

shutil.copy(source_file, destination_file)

with open(destination_file, 'w') as f:
    f.write(content)

# Replace values in inventory file
with open('inventory', 'r') as f:
    content = f.read()
content = content.replace('<server_ip>', server_ip)
content = content.replace('<username>', username)
if use_password == 'y':
    ssh_password = input("Enter SSH password: ")
    content = content.replace(
        'ansible_ssh_private_key_file = <path/to/private/key>', '')
    content += f'ansible_ssh_pass = {ssh_password}\n'
else:
    private_key_path = input("Enter path to private key: ")
    content = content.replace('<path/to/private/key>', private_key_path)
with open('inventory', 'w') as f:
    f.write(content)

# Run the playbook
subprocess.run(['ansible-playbook', 'main.yml'])