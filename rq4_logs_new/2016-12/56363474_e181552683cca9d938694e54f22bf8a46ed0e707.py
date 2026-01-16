#!/usr/bin/env python
# encoding: utf-8

# import json
import requests
from itertools import izip
from search.parse import parse
from search.clean_sentence import cleaned_sentence
from django.conf import settings

ADDR = settings.ADDR
ROWS = 100000

Gflag = False


def addCluster(inMap, retList, title, dictc={}):
    if title not in inMap:
        retId = inMap[title] = len(retList)
        retList.append(dict({'id': retId, 'count': 1, 'title': title}, **dictc))
        flag = True
    else:
        retId = inMap[title]
        retList[retId]['count'] += 1
        flag = retList[retId]['count'] <= 10
    return (retId, flag)


def addResult(arg, res, pos):
    qpos = [i for i, qm in enumerate(res) if not qm.isupper()]
    amod = [qm if i in qpos else '<a pos=%d href="#">' % i + qm + '</a>'
            for i, qm in enumerate(res)]
    title = ' '.join(res)
    (retId, flag) = addCluster(arg['senmap'], arg['senlist'], title,
                               {'display': ' '.join(amod), 'pos': qpos, 'other': '_other_' in res})
    if flag:
        pos = [i for i in pos if 0 <= i and i < len(arg['sent'])]
        markSent = cleaned_sentence(arg['sent'], pos)
        arg['strlist'].append({'sentence': markSent, 'sen': retId})


def result_part(retJson):
    if len(retJson['desc']['sen']) > 20:
        # retJson['desc']['sen'] = retJson['desc']['sen'][:20] + retJson['desc']['sen'][-1:]
        retJson['desc']['sen'] = retJson['desc']['sen'][:20]
        senlist = [sen['id'] for sen in retJson['desc']['sen']]
        retJson['result'] = [result for result in retJson['result'] if result['sen'] in senlist]

    
def get_mod(ind, mod):
    if ind.isdigit():
        return mod[int(ind)]
    else:
        return ind


def unpack_res(res_sen, mod, nxt, cnt=[0]):
    res_0 = []
    res_1 = []
    res_pos = []
    for res in res_sen:
        if isinstance(res[0], list):
            packed = unpack_res(res, mod, nxt, cnt)
            res_0 += packed[0]
            res_1 += packed[1]
            res_pos += packed[2]
        else:
            res_1.append(res[1])
            if res[0].isdigit():
                if nxt == cnt[0]:
                    res_0.append(res[1])
                    res_pos.append(res[2])
                else:
                    res_0.append(mod[int(res[0])])
                cnt[0] += 1
            else:
                res_0.append(res[0])
                res_pos.append(res[2])
    return (res_0, res_1, res_pos)


def check_token(keys, j, res_S, arg_c):
    tokens = [[], [], []]
    tokens[0] = res_S[j][0].split()
    tokens[1] = res_S[j][1].split()
    tokens[2] = [int(ps) for ps in res_S[j][2].split()]
    res_sen = []
    cpkeys = keys[:]
    flag = False
    
    for i, token in enumerate(izip(tokens[0], tokens[1], tokens[2])):
        if token[1] in cpkeys:
            res_sen.append([token[1], token[1], token[2]])
            flag = True
            cpkeys.remove(token[1])
        else:
            res_sen.append([token[0], token[1], token[2]])
    
    if not flag:
        return ([], False)
    if cpkeys:
        for i, token in enumerate(res_sen):
            if token[0].isdigit():
                ind = int(token[0])
                if ind in arg_c:
                    (child_sen, child_flag) = check_token(cpkeys, arg_c[ind], res_S, arg_c)
                    if child_flag:
                        res_sen[i] = child_sen
                        return (res_sen, True)
        return ([], False)
    else:
        return (res_sen, True)
        
        
def extend_token(arg, node, res_S, child_sen, mod):
    for res in res_S:
        tokens = [res[0].split(), res[1].split(), [int(ps) for ps in res[2].split()]]
        if tokens[0][-1] == node or node not in tokens[0]:
            continue
        res_sen = []
        for i, token in enumerate(izip(tokens[0], tokens[1], tokens[2])):
            if token[0] == node:
                res_sen += child_sen
            else:
                res_sen.append([token[0], token[1], token[2]])
        (res_0, res_1, res_pos) = unpack_res(res_sen, mod, arg['nxt'])
        if arg['type'] == 1:
            addResult(arg, res_1, res_pos)
        elif arg['type'] == 0:
            addResult(arg, res_0, res_pos)            


def get_comnex_db(tokens, key, args):
    retJson = {'result': [], 'desc': {'sen': []}}
    senmap = {}

    keys = [tokens[k]['lemma'] for k in key]
    t1key = args['title']
    keys.sort(key=lambda word: -len(word))
    sents = '+AND+'.join(keys)
    cnt = 0

    while True:
        rs = requests.get('http://%s:8983/solr/syntax4/select?q=res2:%s&wt=json&start=%d&rows=%d' % (ADDR, sents, cnt, ROWS))
        cnt += ROWS
        rs = rs.json()['response']['docs']
        if len(rs) == 0:
            break

        for sen in rs:
            mod = sen['tokens']
            sent = sen['sent'].split()
            res1 = sen['res1']
            res2 = sen['res2']
            res3 = sen['res3']
            if not (len(res1) == len(res2) == len(res3)):
                continue
            res_S = [(res1[i], res2[i], res3[i]) for i in range(len(res1))]
            
            arg = dict({
                'senmap': senmap,
                'senlist': retJson['desc']['sen'],
                'strlist': retJson['result'],
                'sent': sent,
            }, **args)
            arg_c = dict()
            for i, i_res1 in enumerate(res1):
                arg_c[int(i_res1.split()[-1])] = i
            
            for i in range(len(res_S)):
                (res_sen, flag) = check_token(keys, i, res_S, arg_c)
                if flag:
                    (res_0, res_1, res_pos) = unpack_res(res_sen, mod, arg['nxt'])
                    if arg['type'] == 1:
                        addResult(arg, res_1, res_pos)
                    elif arg['type'] == 0:
                        addResult(arg, res_0, res_pos)
                    if ' '.join(res_1) == t1key:
                        extend_token(arg, res1[i].split()[-1], res_S, res_sen, mod)

    retJson['desc']['sen'].sort(key=lambda word: -word['count'] if not word['other'] else 0)
    result_part(retJson)
    return retJson


def get_ftree_mod_inter(sentence, key, ctype, nxt=-1):
    rquest = parse(sentence)
    tokens = rquest['sentences'][0]['tokens']

    return get_comnex_db(tokens, key, {'title': sentence, 'type': ctype, 'nxt': nxt})