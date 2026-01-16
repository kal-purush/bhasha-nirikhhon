"""Parsing the message seen on a server."""
from stats_class import Stats_recorder
from insults_class import Insults
import bot_functions as bf
from authenticate import RESPONSE_TYPE

# CONSTANTS
COMMAND_PREFIX = 'WHOA'
#

insults = Insults()


def is_command(msg):
    """Parse to see if command."""
    if msg.upper().startswith(COMMAND_PREFIX):
        return (True, msg[len(COMMAND_PREFIX) + 1:])
    return (False, msg)


def formatted_correctly(message):
    """."""
    bracket_open = ['{', '[', '(', '"""', '"', '/*', '<']
    bracket_close = ['}', ']', ')', '"""', '"', '*/', '>']
    lst_state = []
    msg = message.content

    ind = 0
    max_l = len(msg)
    while ind < max_l:
        cond = True
        for ch in bracket_open:
            if msg[ind:ind + len(ch)] == ch:
                lst_state.append(bracket_open.index(ch))
                ind += len(ch)
                cond = False
                break

        if lst_state:
            ch = bracket_close[lst_state[-1]]
            char_msg = msg[ind:ind + len(ch)]
            if char_msg in bracket_close:
                if msg[ind:ind + len(ch)] != ch:
                    return message.author.mention + insults.get()
                del lst_state[-1]
                ind += len(ch)
                cond = False
                continue

        if cond:
            # Normal character
            ind += 1

    lst_state.reverse()
    return "".join([bracket_close[x] for x in lst_state])


class Message_parser(object):
    """Parser for the message."""

    stats_obj = Stats_recorder()

    def __init__(self):
        """."""
        # todo
        return

    def parse_message(self, message):
        """Function that parse the message."""
        msg_tup = is_command(message.content)
        # This is a command
        if msg_tup[0]:
            if msg_tup[1].find('stats_perc') == 0:
                return RESPONSE_TYPE['MESSAGE'], self.stats_obj.str_stats_perc(message.server.id)

            if msg_tup[1].find('stats') == 0:
                return RESPONSE_TYPE['MESSAGE'], self.stats_obj.str_stats(message.server.id)

            if msg_tup[1].find('invite') == 0:
                return 0, ''

            return bf.parse_commands(msg_tup[1], message.author.mention)

        # This is not a command
        msg = formatted_correctly(message.content)
        if msg != '':
            return RESPONSE_TYPE['MESSAGE'], msg

        self.stats_obj.add(message.server.id, message.author.mention)
        return -1, ''