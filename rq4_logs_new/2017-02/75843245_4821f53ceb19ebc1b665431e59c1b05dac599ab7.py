import os
import random

class Command:
    def __init__(self, bot):
        self.trigger = "slap"
        self.bot     = bot


    def impl(self, *args):
        with open("slaps.txt", "ab+") as fl:
            fl.seek(0, os.SEEK_END)
            size = fl.tell()

            position = int(random.random() * size)
            fl.seek(position)

            while fl.read(1) != b'\x0A':
                position -= 1

                if position < 0:
                    position = 0
                    fl.seek(0)
                    break

                fl.seek(position)

            return "{name} " + "slaps {target} around a bit with {thing}".format(
                target=args[0],
                thing=fl.readline().decode('utf-8')
            )


    def run(self, api, update, args):
        target = "#schizo"
        if len(args) > 0:
            target = args[0]

        api.sendMessage(
            chat_id = update.message.chat_id,
            text    = self.impl(target).format(
                name = update.message.from_user.first_name
            )
        )