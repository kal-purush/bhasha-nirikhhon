import sys
import os

from invoke import task

PYENV_ROOT = '/usr/local/pyenv'
PYENV_PROFILE = '/etc/profile.d/pyenv.sh'
PIP_VERSION = '10.0.1'
PIP_TOOLS_VERSION = '1.9.0'
PY_VERSIONS = ['3.6.5']


@task
def grid(ctx, docs=False, port='4444'):
    """Starts Selenium HUB, firefox node and chrome node docker containers,
       if they're not already running.
       Creates 'grid' docker network unless already exists.

       :param str port: selenium hub port.
    """
    if 'selenium-hub' not in ctx.run('docker ps -a').stdout:
        docker_networks = ctx.run('docker network ls')
        if 'grid' not in docker_networks.stdout:
            ctx.run('docker network create grid')
        grid_cmd = """
            docker run -d -p {port}:{port} --name selenium-hub selenium/hub &&
            docker run -d --link selenium-hub:hub -v /dev/shm:/dev/shm selenium/node-chrome &&
            docker run -d --link selenium-hub:hub -v /dev/shm:/dev/shm selenium/node-firefox""".format(port=port)
        ctx.run(grid_cmd)


@task
def webtests(ctx, testpath='', browsers='all', processes='', te_id='', localmode='', te_remove=''):
    """Incrementally executes speicified selenium/pytest test cases with specified browsers.

       :param str testpath: path to specific pytest modules or folders with tests.
        Usage: '--testpath /vagrant/selenium/.../test.py'.
       :param str browsers: list of browsers on which the test should be executed.
        Usage: '--browsers firefox,chrome,...'.
       :param str processes: number of processes for parallel testing.
        Usage: '--processes 3'.
       :param str te_id: specify id for already created TestEnv.
        Usage: '--te-id <id>'.
       :param localmode str: for test runs on local machine.
        Usage '--localmode true'.
       :param te_remove str: always destroy created TestEnv.
        Usage '--te-remove true'.
    """
    ctx.run(r'find . -name "*.pyc" -exec rm -f {} \;')
    browsers = ['firefox', 'chrome'] if browsers == 'all' else browsers.split(',')
    processes = ' -n %s' % processes if processes else ''
    te_id = '--te-id %s' % te_id if te_id else ''
    te_remove = '--te-remove true' if te_remove else ''
    for browser in browsers:
        driver = browser if localmode else 'Remote'
        command = 'python3 -m pytest%s --driver %s --host 0.0.0.0 --port 4444 --capability browserName %s %s %s %s --disable-warnings' %\
            (processes, driver, browser, testpath, te_id, te_remove) #FIX ME - Deals with using deprecated options in third-party libraries (mainly pluggy)
        print(command)
        ctx.run(command)


@task
def seleniumdrivers(ctx):
    """Install geckodriver and chromedriver for local machine selenium runs.
    """
    if sys.platform == 'darwin':
        urls = [
            'https://chromedriver.storage.googleapis.com/2.41/chromedriver_mac64.zip',
            'https://github.com/mozilla/geckodriver/releases/download/v0.18.0/geckodriver-v0.18.0-macos.tar.gz']
    elif sys.platform == 'linux':
        urls = [
            'https://chromedriver.storage.googleapis.com/2.41/chromedriver_linux64.zip',
            'https://github.com/mozilla/geckodriver/releases/download/v0.18.0/geckodriver-v0.18.0-linux64.tar.gz']
    else:
        raise NotImplementedError('Your OS is not MacOS or Linux type. You need to install chromedriver and geckodriver manually.')
    for url in urls:
        name = 'chromedriver' if 'chromedriver' in url else 'geckodriver'
        ctx.run('wget %s' % url)
        ctx.run('tar -xvzf %s*' % url.split('/')[-1])
        ctx.run('chmod +x %s' % name)
        ctx.run('rm -rf %s' % url.split('/')[-1])
        ctx.run('mv {0} /usr/local/bin/{0}'.format(name))