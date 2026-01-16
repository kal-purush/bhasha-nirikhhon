import os
import sys

import apprise

if sys.version_info[0] == 3:
    from urllib.request import urlopen
else:
    from urllib import urlopen

# gotify settings
gotify_address = None
gotify_token = None


def check_online_ip4():
    try:
        with urlopen("https://api.ipify.org") as url:
            data = url.read()
        s = str(data).replace("b'", "").replace("'", "")
    except Exception as e:
        s = e
        s = None
    return s


def check_offline_ip4():
    if os.path.exists("/opt/ip4.txt") and os.path.isfile("/opt/ip4.txt"):
        f = open("/opt/ip4.txt")
        lines = f.readlines()
        if len(lines):
            s = str(lines[-1].rstrip('\r\n'))
        else:
            s = None
    else:
        s = None
    return s


def update_ip4_file(input, update):
    if input and update:
        with open("/opt/ip4.txt", 'a') as file:
            file.write(str(input + '\n'))


def check_ip_change(old, new):

    if not new:
        return False

    elif not old:
        return True

    elif str(old) != str(new):
        print(old)
        print(new)
        return True

    else:
        return False


def get_message(old, new, ip4_change):

    if not ip4_change:
        return

    message = ["DBB Homelab has a new external IP address."]

    if old:
        message.append("Old IP: " + str(old))

    if new:
        message.append("New IP: " + str(new))

    return '    '.join(message)


def send_message(message):

    if not message:
        return

    print(message)

    apobj = apprise.Apprise()
    apobj.add('gotify://' + gotify_address + '/' + gotify_token)
    apobj.notify(
        body=message,
        title='Homelab',
    )


def run():

    # get IP address from file
    offline_ip4 = check_offline_ip4()

    # get external IP address
    online_ip4 = check_online_ip4()

    # check IP changes
    ip4_change = check_ip_change(offline_ip4, online_ip4)

    # update files
    update_ip4_file(online_ip4, ip4_change)

    # get notification message
    message = get_message(offline_ip4, online_ip4, ip4_change)

    # send notification
    send_message(message)


run()