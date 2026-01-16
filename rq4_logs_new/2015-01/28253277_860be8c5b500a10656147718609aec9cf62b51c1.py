#!/usr/bin/env python
import sys
import pystache
import re
import shlex
import json
from copy import deepcopy
from pprint import pprint

TEMPLATE="""
{{#process}}
[program:{{name}}]
command=env {{ command }}
autostart=true
autorestart=true
stdout_logfile=syslog
stderr_logfile=syslog
environment={{env}}
directory={{ cwd }}
user={{ user }}

{{/process}}
[group:{{ app_name }}]
programs={{process_names}}
"""

PROCFILE_LINE = re.compile(r'^([A-Za-z0-9_-]+):\s*(.+)$')
def parse_procfile(contents):
    """
    parse_procfile(str()) -> [{"name": str(), "command": str()}]
    """
    p = []
    for line in contents.splitlines():
        m = PROCFILE_LINE.match(line)
        if m:
            p.append({"name": m.group(1), "command": m.group(2)})
    return p


def parse_env(content):
    """
    Parse the content of a .env file (a line-delimited KEY=value format) into a
    dictionary mapping keys to values.
    """
    values = {}
    for line in content.splitlines():
        lexer = shlex.shlex(line, posix=True)
        lexer.wordchars += '/.+-():'
        tokens = list(lexer)

        # parses the assignment statement
        if len(tokens) != 3:
            continue
        name, op, value = tokens
        if op != '=':
            continue
        if not re.match(r'[A-Za-z_][A-Za-z_0-9]*', name):
            continue
        values[name] = value

    return values


def io_load_env():
    """
    io_load_env() -> dict(str(), str())
    """
    content = open("./.env").read()
    return parse_env(content)


def io_load_procfile():
    """
    io_load_procfile() -> dict(str(), str())
    """
    content = open("./Procfile").read()
    return parse_procfile(content)
    

def io_load_manifest():
    """
    io_load_manifest() -> {"app_name": str(), "cwd": str(), "user": str()}
    """
    return json.load(open("./.slug-manifest.json"))


def map_context(manifest, processes, env):
    def format_env((k,v)):
        return "{0}={1!r}".format(k,v)
    ctx = {
        "app_name": manifest['app_name'],
        "process": []
    }
    env = map(format_env, env.items())
    process_names = []
    for process in processes:
        process_names.append(process['name'])
        process['env'] = deepcopy(env)
        # add port 5000 if this is a web process
        if process['name'] == "web":
            process['env'].append("PORT=5000")
        process['env'] = ", ".join(process['env'])
        process['user'] = manifest['user']
        process['cwd'] = manifest['cwd']
        ctx['process'].append(process)
    ctx['process_names'] = ", ".join(process_names)
    return ctx


def main():
    env = io_load_env()
    manifest = io_load_manifest()
    processes = io_load_procfile()
    ctx = map_context(manifest, processes, env)
    out_contents = pystache.render(TEMPLATE, ctx)
    out_file = "{0}/{1}.conf".format(sys.argv[1], manifest['app_name'])
    open(out_file, "w").write(out_contents)

if __name__ == '__main__':
    main()