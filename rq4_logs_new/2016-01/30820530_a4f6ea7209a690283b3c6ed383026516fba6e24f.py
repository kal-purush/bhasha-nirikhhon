from remote import *

def cmd(cmdstr, retries=1):
    '''
    Run cmd on localhost, supposed to be set up for IRODS access.

    Return triple of (status, stdout, stderr).
    '''
    if isinstance(cmdstr,list): cmdstr = ' '.join(cmdstr)
    return command(cmdstr, retries)

def stat(path, host = "localhost", user = os.environ.get('USER')):
    '''
    Return output of the "stat" command run as user@host.

    See cmd() for return values. 
    '''
    return cmd("ils %s" % path)

def rsync(src,dst):
    '''
    Copy a remote file/dir "src" to a destination "dst".
    '''
    cmdstr = "irsync -ra %s %s" % (src, dst)
    return cmd(cmdstr)

def mkdir(target_dir, host, user):
    '''
    Execute mkdir on a remote <user>@<host> connection
    '''
    cmdstr = 'imkdir -p %s' % target_dir
    return cmd(cmdstr)

def scp(src, dst):
    cmdstr = "icp -p %s %s" % (src, dst)
    return cmd(cmdstr)

