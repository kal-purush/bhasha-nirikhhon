from subprocess import *

directory_to_mount = "192.168.0.3:/mnt/nfsserver"
mount_location = "/mnt/nfs"

def nfs_mount(directory_to_mount, mount_locaion):
    command = 'mount -t nfs ' + directory_to_mount + " " + mount_location + " -o noauto,x-systemd.automount"
    check_call(command, shell = True)