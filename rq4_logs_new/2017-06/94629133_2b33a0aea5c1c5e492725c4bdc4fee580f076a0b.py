# -*- coding: utf-8 -*-
from slackbot.bot import Bot, respond_to, listen_to
import re
import json


@respond_to('.*')
def help(message):
    message.reply("못알아먹겠소")


@listen_to('github')
@respond_to('github', re.IGNORECASE)
def github(message):
    attachments = [{
        'fallback': '깃허브',
        'author_name': '9to6',
        'author_link': 'https://github.com/9to6',
        'text': '이 사이트 말씀하신건가요?\nhttps://github.com/9to6',
        'color': '#59afe1'
    }]
    message.reply_webapi('혹시 이거?', json.dumps(attachments))


def main():
    bot = Bot()
    bot.run()

if __name__ == "__main__":
    main()