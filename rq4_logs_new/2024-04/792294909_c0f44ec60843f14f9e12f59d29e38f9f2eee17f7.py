#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A minimal web interface to configure, start and stop the KISSHOME traffic capturing service.

It provides these endpoints:
* update_config: set credentials/configuration
* start_capture: start the traffic capture service
* stop_capture: stop the traffic capture service
* check_traffic_status: check if the service is running
"""

import subprocess
import json
from flask import Flask, request, render_template, redirect, url_for, jsonify
from hashlib import md5

app = Flask(__name__)

# Configuration file path
CONFIG_FILE = '/etc/kisshome/config.json'

def load_config():
    """
    Load configuration from the JSON file.
    """
    try:
        with open(CONFIG_FILE, 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        return {}

def save_config(config):
    """
    Save configuration to the JSON file
    """
    with open(CONFIG_FILE, 'w') as config_file:
        json.dump(config, config_file, indent=4)

def validate_mac_addresses(mac_addresses):
    """
    Returns True if the string is empty or contains only comma-separated MAC addresses of the form 12:34:56:aa:bc:cd.
    """
    # check if the string is empty
    if mac_addresses == "":
        return True
    # check if the string contains only comma separated MAC addresses
    mac_addresses = mac_addresses.split(",")
    for mac_address in mac_addresses:
        if not len(mac_address) == 17:
            return False
        for i in range(0, 17):
            if i in [2, 5, 8, 11, 14]:
                if not mac_address[i] in "-:":
                    return False
            elif not mac_address[i] in "0123456789ABCDEFabcdef":
                return False
    return True


def get_session_id(fritzboxip, fritzboxuser, fritzboxpwd):
    """
    Verify that the credentials can be used to login to the FritzBox
    """
    # Request challenge token from Fritz!Box
    try:
        challenge_response = subprocess.check_output(['curl', '-k', '-s', fritzboxip + '/login_sid.lua']).decode('utf-8')
    except subprocess.CalledProcessError:
        # This occurs when the IP is unreachable or doesn't respond with the desired information.
        return "IP Error"
    
    CHALLENGE = challenge_response.split("<Challenge>")[1][:8]
    
    # Create an authentication token by hashing challenge token with password
    
    challenge_password = CHALLENGE + '-' + fritzboxpwd
    challenge_password = ''.join([char + chr(0) for char in challenge_password])
    HASH = md5(challenge_password.encode()).hexdigest()

    login_response = subprocess.check_output(['curl', '-k', '-s', fritzboxip + '/login_sid.lua', '-d', 'response=' + CHALLENGE + '-' + HASH, '-d', 'username=' + fritzboxuser]).decode('utf-8')
    SID = login_response.split("<SID>")[1][:16]

    # Check for successful authentication
    return SID != '0000000000000000'


@app.route('/')
def index():
    config = load_config()
    fritz_password = config.get('fritz_password', '')
    fritz_username = config.get('fritz_username', '')
    fritz_ip = config.get('fritz_ip', '')
    filtered_macs = config.get('filtered_macs', '')
    interface = config.get('interface', '')

    credential_message = request.args.get('credential_message', '')
    service_message = request.args.get('service_message', '')
    return render_template('index.html', fritz_password=fritz_password, fritz_username=fritz_username, fritz_ip=fritz_ip, filtered_macs=filtered_macs, interface=interface, credential_message=credential_message, service_message=service_message)

@app.route('/update_config', methods=['POST'])
def update_config():
    config = load_config()

    new_password = request.form.get('fritz_password')
    new_username = request.form.get('fritz_username')
    new_ip = request.form.get('fritz_ip')
    new_iface = request.form.get('interface', '1-guest')
    new_filter = request.form.get('filtered_macs').replace(' ', '')
    new_filter = new_filter.replace("-", ":")

    # Check if credentials are valid
    fritzbox_sid = get_session_id(new_ip, new_username, new_password)
    if fritzbox_sid == "IP Error":
        message = "FritzBox nicht erreichbar. Falsche IP-Adresse? Default-IP ist 192.168.178.1"
    elif fritzbox_sid:
        # Do some sanity checks on the new configuration values/credentials
        # and if everything looks ok, save them in the config file.
        if not validate_mac_addresses(new_filter):
            message = "Das Format der MAC-Adressen im Feld filtered_macs stimmt nicht. MAC-Adressen muessen im Format 12:34:56:aa:bc:cd vorliegen und komma-separiert sein."
        else:
            config['fritz_password'] = new_password
            config['fritz_username'] = new_username
            config['fritz_ip'] = new_ip
            config['filtered_macs'] = new_filter
            config['interface'] = new_iface
            save_config(config)
            message = "Credentials verified and set."
    else:
        message = "Couldn't verify credentials."

    
    return redirect(url_for('index', credential_message=message))

@app.route('/start_capture', methods=['POST'])
def start_capture():
    proc = subprocess.run('sudo systemctl enable kisshome-traffic-logger.service'.split()).returncode
    if not proc:
        proc = subprocess.run('sudo systemctl start kisshome-traffic-logger.service'.split()).returncode
        if not proc:
            message = 'Service started successfully.'
        else:
            message = 'Error.'
    else:
        message = 'Error.'
    return redirect(url_for('index', service_message=message))

@app.route('/stop_capture', methods=['POST'])
def stop_capture():
    proc = subprocess.run('sudo systemctl disable kisshome-traffic-logger.service'.split()).returncode
    if not proc:
        proc = subprocess.run('sudo systemctl stop kisshome-traffic-logger.service'.split()).returncode
        if not proc:
            message = 'Service stopped successfully.'
        else:
            message = 'Error.'
    else:
        message = 'Error.'
    return redirect(url_for('index', service_message=message))

# Check if the kisshome service is running
def is_kisshome_running():
    try:
        # If the return code was non-zero it raises a CalledProcessError
        # systemctl status kisshome-traffic-logger returns 0 when running and 3 when stopped
        subprocess.check_output(['systemctl', 'status', 'kisshome-traffic-logger'])
        return True
    except subprocess.CalledProcessError:
        return False

@app.route('/check_traffic_status')
def check_traffic_status():
    is_running = is_kisshome_running()
    status_message = 'Running' if is_running else 'Not Running'
    return jsonify({'status': status_message})


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
