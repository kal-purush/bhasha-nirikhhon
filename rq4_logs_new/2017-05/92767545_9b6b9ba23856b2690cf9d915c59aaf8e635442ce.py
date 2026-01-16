import click
import requests
import json
import os
import subprocess
import re
import uuid
import datetime
import time
import config_pb2

port = os.environ.get('WEB_SERVER_PORT')
if port == None:
    port = '8081'
    os.environ['WEB_SERVER_PORT'] = '8081'

url = 'http://localhost:' + port
default_bucket = 's3:CBFS/'
rclone = '/usr/bin/rclone'
cbfs_cfg_dir = os.environ['HOME'] + '/.cbfs/'

"""""""""""""""""""""""""""""""""""""""""""""""""""""
Helper functions
"""""""""""""""""""""""""""""""""""""""""""""""""""""
def create_config(bucket, fsid):
    initial_cfg = config_pb2.Config()
    initial_cfg.name = fsid
    initial_cfg.uuid = fsid
    initial_cfg.primary.host = "localhost"
    initial_cfg.primary.epoch = 0
    initial_cfg.primary.startTime = int(time.time())
    initial_cfg.primary.fuse_cache_enable = True
    initial_cfg.primary.fuse_direct_io = False
    initial_cfg.keyValueStore.type = 1 #LEVELDB
    initial_cfg.keyValueStore.persistenceRule = 2 #LAZY
    initial_cfg.keyValueStore.maxSizeBytes = 100000000
    initial_cfg.tiering.cacheDir = 'proxy_dir' #'/var/cbfs/' + fsid
    initial_cfg.cloud.protocol = 1 #S3
    initial_cfg.cloud.endpoint = bucket + fsid
    return str(initial_cfg);
    
def get_filesystem_list(bucket, tag):
    match_list = []
    try:
        output = subprocess.check_output([rclone, "lsd", "-q", bucket])
    except subprocess.CalledProcessError as e:
        "Let caller handles the error"
        raise
        
    fs_list = output.split('\n')
    """
    for fs in fs_list:
        obj = re.search(tag, fs)
        if obj != None:
            fs_name = fs.lstrip().split(' ')[1].split('/')[0]
    """
    for fs in fs_list:
        obj = re.search(tag, fs)
        if obj != None:
            fs_name = fs[obj.start():]
            match_list.append(fs_name)

    return match_list

def create_filesystem(bucket, tag):
    result = 0
    while True:
        "1. generate uuid"
        uuid_str = str(uuid.uuid1())
        fs_id = str(tag) + '-' + uuid_str
        fs_path = bucket + fs_id
        "2. check if the filesystem id exist"
        try:
            fs_list = get_filesystem_list(bucket, fs_id)
        except subprocess.CalledProcessError as e:
            click.echo("Unable to talk to the cloud")
            raise

        if fs_list == []:
            "filesystem id is unique"
            "3. create the metadata file"
            tmpdir = '/tmp/' + fs_id + '/'
            
            result = subprocess.call(["mkdir", tmpdir])
            if result == 1:
                break;
            
            "TODO: additional configuration"
            config = {'filesystem_id': fs_id,
                      'creation_time': str(datetime.datetime.utcnow()),
                      'mount': 0}
            f = open(tmpdir + 'metadata', 'w')
            f.write(json.dumps(config))
            os.fsync(f)
            f.close()
            
            "4. create the initial config for the backend"
            fs_config = create_config(bucket, fs_id)
            f = open(tmpdir + fs_id + ".cfg", 'w')
            f.write(fs_config);
            os.fsync(f)
            f.close()
            
            "5. create the directory on the cloud and copy the metadata over"
            try:
                subprocess.check_output([rclone, "-q", "mkdir", fs_path])
            except subprocess.CalledProcessError as e:
                click.echo("Unable to create filesystem on cloud")
                raise
            try:
                subprocess.check_output([rclone, "-q", "copy", tmpdir, fs_path])
            except subprocess.CalledProcessError as e:
                click.echo("Unable to store filesystem config on cloud")
                raise
            "6. remove the temporary metadata file"
            try:
                subprocess.check_output(["rm", "-r", tmpdir])
            except:
                return
            break;
        
    return [result, fs_id]

def delete_filesystem(bucket, fs):
    "TODO check if the filesystem is mounted"
    try:
        fs_path = bucket + fs
        subprocess.check_output([rclone, "-q", "delete", fs_path])
    except subprocess.CalledProcessError as e:
        click.echo("Unable to remove filesystem from cloud")
        raise

def mount_filesystem(bucket, fs, mount_point):
    "0. check if kvfs binary exist"
    try:
        kvfs = os.environ['KVFS']
    except:
        click.echo("Unable to mount.  KVFS environment needs to be specify")
        return
    
    "1. check if filesystem exists"
    fs_list = get_filesystem_list(bucket, fs)
    fs_cfg = fs + '.cfg'
    cloud_fs_cfg = bucket + fs + '/' + fs_cfg
    local_fs_cfg = cbfs_cfg_dir + fs_cfg
    if fs_list == []:
        click.echo("Unable to mount! Filesystem doesn't exist");
        return False
    "2. check if filesystem is mounted already - TODO"
    "3. check if mount point exist"
    if os.path.exists(mount_point) == False:
        click.echo("Unable to mount! mount point doesn't exist");
        return False
    "4. check if the cbfs configuration directory exists, if not, create one"
    if os.path.exists(cbfs_cfg_dir) == False:
        os.mkdir(cbfs_cfg_dir)
    "5. bring the FS config from cloud if necessary"
    if os.path.isfile(local_fs_cfg) == False:
        try:
            subprocess.call([rclone, '-q', "copy", cloud_fs_cfg, cbfs_cfg_dir])
        except subprocess.CalledProcessError as e:
            click.echo("Unable to copy filesystem config from cloud")
            raise
    "TODO: change the configuration per user parameters"
    "6. start the kvfs process"
    click.echo("starting kvfs process...")
    try:
        subprocess.Popen([kvfs, '--config_file='+local_fs_cfg, '-s', mount_point])
    except subprocess.CalledProcessError as e:
        click.echo("Unable to launch KVFS process")

def umount_filesystem(mount_point):    
    "1. check if filesystem is mounted already - TODO"
    "2. check if mount point exist"
    if os.path.exists(mount_point) == False:
        click.echo("Unable to mount! mount point doesn't exist");
        return False
    try:
        subprocess.call(['fusermount', '-u', mount_point])
    except subprocess.CalledProcessError as e:
        click.echo("Unable to umount filesystem")
        raise
    
"""
CLI Entry point
"""
@click.group()
def cli():
    pass

"""
Filesystem related commands
"""
@click.command()
@click.argument('action', type=click.Choice(['create', 'delete', 'list', 'mount', 'umount', 'status', 'evict', 'evict-status', 'evict-policy']))
@click.option('--fsid', default='', help='system generated filesystem id given during filesystem creation')
@click.option('--mount-point', default='', help='directory where the filesystem should be mounted')
@click.option('--mode', default='', help="READ only, READ+Write mode")
@click.option('--cache-size', default='', help="size of the proxy cache")
@click.option('--checkpoint-interval', default='', help="interval at which the fileystem should be checkpointed")
@click.option('--tag', default='', help="user defined tag")

def filesystem(action, fsid, mount_point, mode, cache_size, checkpoint_interval, tag):
    uri = url + '/filesystem'
    """filesystem commands\n
    ACTION:       create|delete|mount|umount|status|evict|evict-status|evict-policy
    """
    if action=='create':
        if tag == '':
            click.echo("Filesystem creation failed! --tag input is required");
            return
        "1. Generate an UUID"
        try:
            [result, fs_id] = create_filesystem(default_bucket, tag)
            click.echo("Filesystem ID:" + fs_id)
        except:
            click.echo("Filesystem creation failed!")
    elif action == 'delete':
        if fsid == '':
            click.echo("Filesystem deletion failed! --fsid is required");
            return
        try:
            delete_filesystem(default_bucket, fsid)
            "TODO check the delete result"
            click.echo("Deleted: " + fsid)
        except:
            click.echo("Filesystem deletion failed")
    elif action == 'list':
        click.echo("filesystem list")
        try:
            fs_list = get_filesystem_list(default_bucket, tag)
        except subprocess.CalledProcessError as e:
            click.echo("Unable to list the filesystems")
            return
        for fs in fs_list:
            click.echo(fs)
    elif action == 'mount':
        if fsid == '':
            click.echo("mount failed! --fsid parameter is required")
            return
        if mount_point == '':
            click.echo("mount failed! --mount-point parameter is required")
            return
        mount_filesystem(default_bucket, fsid, mount_point)
        #3. perform the action mount
    elif action == 'umount':
        #1. verify filesystem id
        #2. verify filesystem is mounted
        if mount_point == '':
            click.echo("umount failed! --mount-point parameter is required")
            return
        umount_filesystem(mount_point)
    elif action == 'status':
        click.echo("not implemented")
        content = {'action':action,
                   'filesystem':fsid}
        result = requests.get(uri, data = content)
        click.echo(result.text)
    elif action == 'evict':
        click.echo("not implemented")
    elif action == 'evict-status':
        click.echo("not implemented")
    elif action == 'evict-policy':
        click.echo("not implemented")
    else:
        click.echo('unknown option')

"""
Snapshot related commands
"""
@click.command()
@click.argument('action', type=click.Choice(['create', 'delete','status', 'list'])) 
@click.argument('filesystem')
@click.option('--tag', default='', help='user defined tag for the snapshot')
@click.option('--snapshotid', default='', help='system generated snapshot ID')
def snapshot(filesystem, action, tag, snapshotid):
    uri = url + "/SNAP"
    """snapshot commands\n
       FILESYSTEM: Filesystem uuid\n
       ACTION:     create|delete|status"""

    if action=='create':
        content = {'filesystem': filesystem,
                   'action': action,
                   'tag': tag}
        result = requests.get(uri, data = content)
        click.echo(result.text)
    elif action == 'delete':
        click.echo('Deleted: ' + snapshotid)
        content = {'filesystem': filesystem,
                   'tag': tag,
                   'action': action,
                   'snapshotid': snapshotid}
        #requests.post(uri, data = content)
        click.echo("not implemented")
    elif action == 'status':
        uri = url + "/STAT"
        content = {'filesystem': filesystem,
                   'tag': tag,
                   'action': action,
                   'snapshotid': snapshotid}
        result = requests.put(uri, data = content)
        click.echo(result.text)
    elif action == 'list':
        click.echo("%s\t%s\t%s\t%s" % ("index", "Snapshot ID", "Tag", "Timestamp"))
        click.echo("not implemented")
    else:
        click.echo('Error Unknown option');

"""
Clone related commands
"""
@click.command()
def clone():
    """clone commands"""
    click.echo("not implemented")

"""
Config related commands
"""
@click.command()
def config():
    """configuration commands"""
    click.echo("not implemented")

"""
Internal commands
"""
@click.command()
def dump():
    uri = url + "/DUMP"
    result = requests.get(uri)
    click.echo(result.text)

"""
Stats related commands
"""
@click.command()
def dbstats():
    uri = url + "/DBSTATS"
    result = requests.get(uri)
    click.echo(result.text)

cli.add_command(snapshot)
cli.add_command(filesystem)
#cli.add_command(clone)
#cli.add_command(config)
cli.add_command(dump)
cli.add_command(dbstats)