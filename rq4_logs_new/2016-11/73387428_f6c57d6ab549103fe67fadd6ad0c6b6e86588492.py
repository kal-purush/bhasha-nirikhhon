#!/usr/bin/env python

# Usage: notify app_name id title body icon timeout_in_ms

import sys
import dbus

notify = dbus.Interface(dbus.SessionBus()
    .get_object("org.freedesktop.Notifications", "/org/freedesktop/Notifications"),
    "org.freedesktop.Notifications")

id = notify.Notify(sys.argv[1], int(sys.argv[2]), sys.argv[5], sys.argv[3], 
        sys.argv[4], "", "", int(sys.argv[6]))

print id