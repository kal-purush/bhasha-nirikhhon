
import os
import logging
import subprocess

logger = logging.getLogger(__name__)
log_level = logging.DEBUG
logger.setLevel(log_level)
console = logging.StreamHandler()
console.setLevel(log_level)
logger.addHandler(console)

logger.debug('crazy-helpful logging enabled...')

def get_proc_output(cmd):
    try:
        cmd = cmd.split()
    except AttributeError:
        pass
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = process.communicate()
    retcode = process.poll()
    if retcode:
        raise Exception('Process said:\n{0} (Exit Val {1})'.format(err, retcode))
    return output