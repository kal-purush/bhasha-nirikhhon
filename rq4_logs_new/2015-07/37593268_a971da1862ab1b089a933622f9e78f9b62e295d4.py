import logging

try:
    from White.Core.WindowsAPI.KeyboardInput import SpecialKeys
except:
    from traceback import format_exc
    logging.error(format_exc())

from _util import IronbotException
from _params import pop

SPECIAL_KEYS = set((
    'ALT', 'BACKSPACE', 'CAPS', 'CONTROL', 'DELETE', 'DOWN', 'END', 'ESCAPE',
    'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9', 'F10',
    'F11', 'F12', 'F13', 'F14', 'F15', 'F16', 'F17', 'F18', 'F19', 'F20',
    'F21', 'F22', 'F23', 'F24',
    'HOME', 'INSERT', 'LEFT', 'LEFT_ALT', 'LWIN', 'NUMLOCK', 'PAGEDOWN', 'PAGEUP', 'PRINT', 'PRINTSCREEN', 'RETURN',
    'RIGHT', 'RIGHT_ALT', 'RWIN', 'SCROLL', 'SHIFT', 'SPACE', 'TAB'
))

class SpecialKey(object):
    def __init__(self, name):
        self.name = name
        self.key = getattr(SpecialKeys, name)

    def hold(self, kbd):
        kbd.HoldKey(self.key)

    def leave(self, kbd):
        kbd.LeaveKey(self.key)

    def press(self, kbd):
        kbd.PressSpecialKey(self.key)



class Key(object):
    def __init__(self, name):
        self.name = name
        self.key = name.upper()

    def hold(self, kbd):
        kbd.HoldKey(self.key)

    def leave(self, kbd):
        kbd.LeaveKey(self.key)

    def press(self, kbd):
        kbd.Enter(self.key)


class String(object):
    def __init__(self, v):
        self.v = v

    def enter(self, kbd):
        kbd.Enter(self.v)


def get_key(s):
    if s.upper() in SPECIAL_KEYS:
        return SpecialKey(s.upper())
    if len(s) == 1:
        return Key(s)
    raise IronbotException("Unknown key '%s'" % s)

def pop_key(params):
    return get_key(pop(params))

def pop_key_string(params):
    return String(pop(params))