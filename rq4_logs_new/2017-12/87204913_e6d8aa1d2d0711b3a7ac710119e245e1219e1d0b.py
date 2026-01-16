#!/usr/bin/python
# -*- coding: utf-8 -*-
# \file cpc.py
# \brief TODO
# \author Florent Guiotte <florent.guiotte@gmail.com>
# \version 0.1
# \date 14 déc. 2017
#
# TODO details

import re
import requests
from bs4 import BeautifulSoup

from sopel import module

cpc_search_url = 'https://coincoinpc.herokuapp.com/search.html?searchInput='
not_found_msg  = '¯\_(ツ)_/¯'

@module.example('.cpc Batman Arkham Knight')
@module.commands('cpc')
def cpc_sopel(bot, trigger):
    """Shows CanardPC's rating of a game with the crowd sourced database coincoinpc.herokuapp.com"""
    args = trigger.split()

    if len(args) < 2:
        return

    search = ' '.join(args[1:])
    bot.say(cpc(search))

def cpc(search):
    result = requests.get(cpc_search_url + search)

    if result.status_code != 200:
        raise ConnectionError('Could not connect to ' + result.url)

    soup = BeautifulSoup(result.content, 'lxml')
    games = soup.find_all('div', 'content')

    if len(games) == 0:
        return not_found_msg

    game = games[-1] # Return the oldest published match

    raw_mark = game.find('div',"ui left pointing red basic label").string
    mark = re.sub('\ {2,}|\n|\t', '', raw_mark).strip()
    title = game.find('a',"header").string.strip()
    desc = game.find('div', 'description').string.strip()

    return '{} - {} - {}'.format(title, desc, mark)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('search', nargs='+')
    args = parser.parse_args()

    print(cpc(' '.join(args.search)))