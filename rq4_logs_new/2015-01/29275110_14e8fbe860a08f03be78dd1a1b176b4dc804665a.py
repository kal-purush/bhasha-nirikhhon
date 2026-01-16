#!/usr/bin/env python3

from flask import Flask, request, send_file
import hashlib
import json
import os
import subprocess
import shutil

conf = {
    'COMPOSER_COMMAND': 'composer',
    'TEMP_DIRS': os.path.join(os.path.dirname(__file__), 'tmp')
}

app = Flask(__name__)


@app.route('/')
def home() -> str:
    return 'Hello world!'


@app.route('/get', methods=['POST'])
def get() -> str:
    if 'data' not in request.form:
        return 'No data provided'
    data = request.form['data']
    try:
        # [{'foo/bar':'1.2.3'}, {'bar/foo':'2.0.*'}]
        deps = json.loads(data)
    except ValueError as e:
        return 'Invalid JSON: %s' % str(e)
    hash_ = hashlib.sha1(data.encode()).hexdigest()
    path = lambda x: os.path.join(conf['TEMP_DIRS'], hash_, x)
    if os.path.isfile(path('vendor.tar.gz')):
        return send_file(path('vendor.tar.gz'))
    composer_params = ['--prefer-dist', '--no-dev']
    # When using the merge-plugin, we have to run `composer install` to fetch it
    # and then `composer update` to actually have it activate. The install step
    # should always be the same, so cache it and copy its contents when creating
    # a new build.
    merge_plugin_cache = os.path.join(conf['TEMP_DIRS'], 'merge-plugin')
    if not os.path.isdir(merge_plugin_cache):
        os.mkdir(merge_plugin_cache)
        cwd = os.getcwd()
        os.chdir(merge_plugin_cache)
        with open('composer.json', 'w') as f:
            json.dump({'require': {'wikimedia/composer-merge-plugin': '0.5.0'}}, f)
        subprocess.check_call(['composer', 'install'] + composer_params)
        os.chdir(cwd)
    shutil.copytree(merge_plugin_cache, path(''))
    for i, required in enumerate(deps):
        with open(path('composer-%s.json' % i), 'w') as f:
            json.dump({
                'require': required
            }, f)
    with open(path('composer.json'), 'w') as f:
        json.dump({
            'require': {
                'wikimedia/composer-merge-plugin': '0.5.0'
            },
            'extra': {
                'merge-plugin': {
                    'include': [
                        'composer-*.json'
                    ]
                }
            }
        }, f)
    cwd = os.getcwd()
    os.chdir(path(''))
    # Fetch dependencies...
    subprocess.check_call(['composer', 'update'] + composer_params)
    # Put the composer.lock file inside vendor for future reference
    shutil.copy('composer.lock', 'vendor/composer.lock')
    # Build tarball...
    subprocess.check_call(['tar', '-czPf', 'vendor.tar.gz', 'vendor'])
    os.chdir(cwd)
    return send_file(path('vendor.tar.gz'))


if __name__ == '__main__':
    app.run(debug=True)