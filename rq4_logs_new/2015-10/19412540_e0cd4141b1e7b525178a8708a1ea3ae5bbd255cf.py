# -*- coding: utf-8 -*-

"""
xfer_scp_pushbullet.py - a weechat script to notify of successful xfer_scp completions

Settings:
    * plugins.var.python.xfer_scp_pushbullet.api_key: your API key for pushbullet
    * plugins.var.python.xfer_scp_pushbullet.channel_tag: the channel_tag you want to publish notifications to

Commands:
    * none

Requires:
    * pushbullet.py (pip install pushbullet.py)

Version: 0.0.1
Author: Grant Bacon <btnarg@gmail.com>
License: GPL3
Date: 01 Oct 2015
"""

import_ok = True

try:
    import weechat
    from pushbullet import Pushbullet
    import re
except:
    print("You must run this script within Weechat!")
    print("http://www.weechat.org")
    import_ok = False

#####
# Registration constants
#####
SCRIPT_NAME = "xfer_scp_pushbullet"
SCRIPT_AUTHOR = "Grant Bacon <btnarg@gmail.com>"
SCRIPT_VERSION = "0.0.1"
SCRIPT_LICENSE = "GPL3"
SCRIPT_DESC = "Notify a pushbullet channel of a successful send from xfer_scp"

####

API_KEY_KEY = "api_key"
CHANNEL_TAG_KEY = "channel_tag"

pb = None
pb_chan = None

def xfer_scp_pushbullet_init():
    api_key = weechat.config_get_plugin(API_KEY_KEY)
    channel_tag = weechat.config_get_plugin(CHANNEL_TAG_KEY)
    if api_key and channel_tag:
        global pb, pb_chan
        pb = Pushbullet(api_key)
        pb_chan = next((chan for chan in pb.channels if chan.channel_tag == channel_tag), None)
        if not pb_chan:
            pb = None
            pb_chan = None
            # throw exception: cannot find channel_tag
    else:
        # throw exception, invalid api_key or missing api_key
        # or jsut show some kind of error in weechat duh
        pb = None
        pb_chan = None

def xfer_scp_pushbullet_config_changed_cb(data, option, value):
    if (option == "plugins.var.python." + SCRIPT_NAME + "." + API_KEY_KEY) or (option == "plugins.var.python." + SCRIPT_NAME + "." + CHANNEL_TAG_KEY):
        xfer_scp_pushbullet_init()

def xfer_scp_success_signal_cb(data, signal, signal_data):
    global pb_chan
    if pb_chan:
        pb_chan.push_note("", signal_data)
        return weechat.WEECHAT_RC_OK
    else:
        return weechat.WEECHAT_RC_ERROR


#### Register, begin
if __name__ == "__main__" and import_ok:
    if weechat.register(SCRIPT_NAME, SCRIPT_AUTHOR, SCRIPT_VERSION, SCRIPT_LICENSE, SCRIPT_DESC, '', ''):
        if not weechat.config_is_set_plugin(API_KEY_KEY):
            weechat.config_set_plugin(API_KEY_KEY, "")

        if not weechat.config_is_set_plugin(CHANNEL_TAG_KEY):
            weechat.config_set_plugin(CHANNEL_TAG_KEY, "")

        weechat.hook_signal("xfer_scp_success", 'xfer_scp_success_signal_cb', '')
        weechat.hook_config("plugins.var.python." + SCRIPT_NAME + ".*", "xfer_scp_pushbullet_config_changed_cb", "")
        xfer_scp_pushbullet_init()