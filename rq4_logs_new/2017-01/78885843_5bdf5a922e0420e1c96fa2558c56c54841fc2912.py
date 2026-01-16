#!/usr/bin/python

import os, sys


if ( len(sys.argv) < 3 ):
   print '\nUSAGE: add-vmachine2cluster <resource_id> <XML filepath>\n'
   sys.exit()
#
filepath = sys.argv[2]
rid = sys.argv[1]

if ( rid[0:3] != 'vm_' ):
   print '\nERROR: resource_id must begin with vm_\n'
   sys.exit()
#
if  not ( os.path.isfile(filepath) ):
   print '\nERROR: filepath was not found.\n'
   sys.exit()
#

pcscmd = 'pcs resource create '
pcscmd += rid + ' '
pcscmd += 'VirtualDomain hypervisor="qemu:///system" '
pcscmd += 'config=' + filepath + ' migration_transport=ssh op start timeout="120s" '
pcscmd += 'op stop timeout="120s" op monitor timeout="30" interval="10" '
pcscmd += 'meta allow-migrate="true" priority="100" op migrate_from interval="0" '
pcscmd += 'timeout="120s" op migrate_to interval="0" timeout="120"'

print pcscmd
os.system( pcscmd )

sys.exit( 0 )

