from subprocess import run
from os.path import isdir

from collections import deque
import json
import os

ignore = ['.git', '.gitignore', '.vscode', '__pycache__', 'README.md']

def make_normal_path(path):
    dq, spl = deque(), [el for el in path.split('/') if len(el) > 0]
    for el in spl:
        if el == '.' and len(dq) == 0:
            dq.append('.')
        elif el == '..':
            dq.pop()
        elif el != '.':
            dq.append(el)
    return '/'.join(dq)

def parse_makefile(filename):
    filename = make_normal_path(filename)
    strings = open(filename, 'r').read().split('\n')
    path = '/'.join(filename.split('/')[:-1])
    dct, pathes = {}, {}
    for string in strings:
        if ':' in string:
            target, dependecy = string.split(':')[0], [el for el in string.split(':')[1:] if el.split('.')[-1] == 'o']
            rpath = make_normal_path('{}/{}'.format(path, target))
            dct[rpath] = dependecy
            pathes[rpath] = filename
    return dct, pathes

def dfs(q, gr):
    priority = {}
    ans = []
    while len(q):
        cur = q.pop()
        ans.append(cur)
        for el in gr[cur]:
            q.push(el)
    return ans

def get_all_makefiles(catalog):
    files = os.listdir(catalog)
    makefiles = []
    for file in files:
        if file in ignore:
            continue
        if file == 'makefile':
            makefiles.append('{}/{}'.format(catalog, file))
        elif isdir('{}/{}'.format(catalog, file)):
            sub_makefiles = get_all_makefiles('{}/{}'.format(catalog, file))
            for makefile in sub_makefiles:
                makefiles.append(makefile)
    return makefiles

def make_priority(path):
    makefiles = get_all_makefiles(path)
    gr, pathes = {}, {}
    q = deque()
    for makefile in makefiles:
        dct, path = parse_makefile(makefile)
        pathes.update(path)
        for key in dct.keys():
            gr[key] = []
            for el in dct[key]:
                gr[el].append(key)
            if len(gr[key]) == 0:
                q.append(key)
    return dfs(q, gr), pathes

def make_filename(path):
    cur_dir = os.getcwd()
    dir = '/'.join(path.split('/')[:-1])
    os.chdir(dir)
    run(["make"])
    os.chdir(cur_dir)

def make_all(path):
    priority, pathes = make_priority(path)
    for el in priority:
        make_filename(el)