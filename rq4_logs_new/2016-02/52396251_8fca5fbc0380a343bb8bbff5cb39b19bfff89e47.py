import os
import re
import signal
from subprocess import Popen, check_output

import jujuresources

from jujubigdata import utils
from charmhelpers.core import templating, hookenv


# Main Flume class for callbacks
class Flume(object):
    """
    This class manages the deployment steps of Flume agent.

    :param DistConfig dist_config: The configuration container object needed.
    """

    def __init__(self, dist_config):
        self.dist_config = dist_config
        self.resources = {
            'flume': 'flume-%s' % utils.cpu_arch(),
        }
        self.verify_resources = utils.verify_resources(*self.resources.values())

    @property
    def config_file(self):
        return self.dist_config('flume_conf') / 'flume.conf'

    def install(self):
        '''
        Create the users and directories. This method is to be called only once.

        :param bool force: Force the installation execution even if this is not the first installation attempt.
        '''
        jujuresources.install(self.resources['flume'],
                              destination=self.dist_config.path('flume'),
                              skip_top_level=True)
        self.dist_config.add_users()
        self.dist_config.add_dirs()
        self.dist_config.add_packages()
        self.setup_flume_config()

    def setup_flume_config(self):
        '''
        copy the default configuration files to flume_conf property
        defined in dist.yaml
        '''
        default_conf = self.dist_config.path('flume') / 'conf'
        flume_conf = self.dist_config.path('flume_conf')
        flume_conf.rmtree_p()
        default_conf.copytree(flume_conf)
        # Now remove the conf included in the tarball and symlink our real conf
        default_conf.rmtree_p()
        flume_conf.symlink(default_conf)

        flume_env = self.dist_config.path('flume_conf') / 'flume-env.sh'
        if not flume_env.exists():
            (self.dist_config.path('flume_conf') / 'flume-env.sh.template').copy(flume_env)

        flume_conf = self.dist_config.path('flume_conf') / 'flume.conf'
        if not flume_conf.exists():
            (self.dist_config.path('flume_conf') / 'flume-conf.properties.template').copy(flume_conf)

        flume_log4j = self.dist_config.path('flume_conf') / 'log4j.properties'
        utils.re_edit_in_place(flume_log4j, {
            r'^flume.log.dir.*': 'flume.log.dir={}'.format(self.dist_config.path('flume_logs')),
        })

    def configure_flume(self, template_data=None):
        '''
        handle configuration of Flume and setup the environment
        '''
        config = hookenv.config()
        templating.render(
            source='flume.conf.j2',
            target=self.config_file,
            context=dict({
                'dist_config': self.dist_config,
                'config': config,
            }, **(template_data or {})),
        )

        flume_bin = self.dist_config.path('flume') / 'bin'
        java_symlink = check_output(["readlink", "-f", "/usr/bin/java"]).decode('utf8')
        java_home = re.sub('/bin/java', '', java_symlink).rstrip()
        with utils.environment_edit_in_place('/etc/environment') as env:
            if flume_bin not in env['PATH']:
                env['PATH'] = ':'.join([env['PATH'], flume_bin])
            env['FLUME_CONF_DIR'] = self.dist_config.path('flume_conf')
            env['FLUME_CLASSPATH'] = self.dist_config.path('flume') / 'lib'
            env['FLUME_HOME'] = self.dist_config.path('flume')
            env['JAVA_HOME'] = java_home

    def run_bg(self, user, command, *args):
        """
        Run a command as the given user in the background.

        :param str user: User to run flume agent
        :param str command: Command to run
        :param list args: Additional args to pass to the command
        """
        parts = [command] + list(args)
        quoted = ' '.join("'%s'" % p for p in parts)
        e = utils.read_etc_env()
        Popen(['su', user, '-c', quoted], env=e)

    def restart(self):
        # check for a java process with our flume dir in the classpath
        if utils.jps(r'-cp .*{}'.format(self.dist_config.path('flume'))):
            self.stop()
        self.start()

    def start(self):
        self.run_bg(
            'flume',
            self.dist_config.path('flume') / 'bin/flume-ng',
            'agent',
            '-c', self.dist_config.path('flume_conf'),
            '-f', self.dist_config.path('flume_conf') / 'flume.conf',
            '-n', 'a1')

    def stop(self):
        flume_pids = utils.jps(r'-cp .*{}'.format(self.dist_config.path('flume')))
        for pid in flume_pids:
            os.kill(int(pid), signal.SIGKILL)

    def cleanup(self):
        self.dist_config.remove_users()
        self.dist_config.remove_dirs()