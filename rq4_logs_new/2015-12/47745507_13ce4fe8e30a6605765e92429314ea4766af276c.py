# coding: utf-8

from collections import namedtuple
from contextlib import contextmanager
from distutils.util import strtobool
from os.path import join
import sys
import urlparse

import requests
from lxml import etree
from fabric.api import cd, env, execute, sudo, task, quiet, run
from fabric.colors import red
from fabric.decorators import runs_once


# TODO:
# 1. deploy specific version (branch / tag / commit)
# 2. IRC notification
# 3. rollback

env.update(
    stage=None,
    sudo_user='ubuntu',
    use_ssh_config=False,
)


class Stage(namedtuple('Stage', 'name hosts has_celery_worker')):

    PARENT_DIR = '/srv'

    @property
    def project_root(self):
        return join(self.PARENT_DIR, self.name)

    @property
    def local_settings_path(self):
        suffix = self.name.replace('hunter-', '')
        return join(self.project_root, 'banker', 'local_settings.%s.py' % suffix)


STAGES = {
    stage.name: stage
    for stage in [
        Stage('hunter-online', hosts=['115.159.159.12', ], has_celery_worker=False),
        # Stage('hunter-dev', hosts=['zerus', ], has_celery_worker=False),
        # Stage('hunter-staging', hosts=['zerus', ], has_celery_worker=False),
    ]
}


@contextmanager
def lock():
    """
    poor man's lock
    """

    with quiet():
        r = sudo('mkdir .deploy.lock')
        if r.return_code != 0:
            sys.exit('Failed to acquire the lock. Maybe someone is deploying.')

    try:
        yield
    finally:
        with quiet():
            sudo('rmdir .deploy.lock')


def update_source(revision, repo):
    sudo('git fetch --quiet %s' % repo)
    sudo('git checkout --quiet %s' % revision)


def copy_local_settings():
    src = env.stage.local_settings_path
    dest = join(env.stage.project_root, 'banker/local_settings.py')
    sudo('cp %s %s' % (src, dest))


def update_deps():
    sudo('make wheel-deps')
    sudo('make clean_pyc')
    sudo('make grunt')


@task
def restart():
    """
    先重启 worker 再重启 Web，避免 Web 新增的 tasks 在 worker 中未定义
    """

    if not env.stage:
        raise Exception('no stage given.')

    name = env.stage.name
    if env.stage.has_celery_worker:
        sudo('sudo /etc/init.d/celery-%s term' % name)  # Celery 只接收 TERM 信号
    sudo('sudo /etc/init.d/%s hup' % name)


@task
def stage(name):
    if name not in STAGES:
        raise Exception('unknown stage name')
    env.stage = STAGES[name]
    env.hosts = env.stage.hosts


@task
def init_deploy(repo_url):
    if not env.stage:
        raise Exception('no stage given.')

    sudo('mkdir -p %s' % env.stage.project_root)

    with cd(env.stage.project_root), lock():
        sudo('git init .')
        sudo('git remote add origin %s' % repo_url)
        sudo('git pull --quiet -u origin master')
        sudo('make venv npm')


@task
def can_i_deploy():
    sudo('echo %s' % red('YES YOU CAN'))


@task
def deploy(revision='origin/master', repo='origin', restart_server='true'):
    if not env.stage:
        raise Exception('no stage given.')

    with cd(env.stage.project_root), lock():
        old_revision = run("git rev-parse HEAD")
        update_source(revision, repo)
        new_revision = run("git rev-parse HEAD")
        update_deps()
        copy_local_settings()

        if strtobool(restart_server):
            execute(restart)
    # execute(notify_bearychat, old_revision, new_revision)


@task
def test():
    with cd('/home'):
        sudo("du -sh")
