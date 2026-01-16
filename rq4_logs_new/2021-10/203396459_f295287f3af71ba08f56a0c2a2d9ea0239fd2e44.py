import codecs
import json
import os
import sys

import django
import pypinyin

sys.path.extend(['../..'])

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Tools.settings")
django.setup()

from Model.Language.Phrase.models import Phrase


def search(query_phrases, as_pts=False):
    if as_pts:
        query_pts = query_phrases
    else:
        query_pts = []
        for phrase in query_phrases:
            phonetic = pypinyin.pinyin(
                hans=phrase,
                errors=lambda _: [None],
                style=pypinyin.Style.NORMAL,
            )
            phonetic = list(map(lambda p: p[0], phonetic))
            query_pts.append(phonetic)
    print('query pts', query_pts)

    for phrase in Phrase.objects.filter(plen__gte=4):
        cand_pt = json.loads(phrase.number_py)
        cand_pt = list(map(lambda p: p[:-1], cand_pt))

        for name in query_pts:
            for phonetic in name:
                if phonetic in cand_pt:
                    break
            else:
                break
        else:
            print(phrase.cy)


search(['孟美岐', '陈令韬'])
# search([['meng', 'men', 'mei', 'qi'], ['chen', 'cheng', 'ling', 'lin', 'tao']], as_pts=True)