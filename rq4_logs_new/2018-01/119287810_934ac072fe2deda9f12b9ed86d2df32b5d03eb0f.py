#!/usr/bin/env python

"""Generate a command and launch etcd"""

import subprocess

import yaml

# Read YAML file of cluster members
with open("cluster-hosts.yml", "r") as hosts:
    hostdata = yaml.load(hosts)

# Get my details
my_hostname = hostdata["me"]["hostname"]
my_address = hostdata["me"]["address"]

clusterstate = "new"

# Generate initial cluster list
clusterlist = []
for hostname, address in hostdata["hostlist"].iteritems():
    clusterlist.append("{hostname}=http://{address}:2380".format(
        hostname=hostname, address=address))

clusterstring = ",".join(clusterlist)

etcd_args = [
    "./etcd",
    "--name", my_hostname,
    "--data-dir", "/var/lib/etcd",
    "--listen-client-urls", "http://{0}:2379".format(my_address),
    "--advertise-client-urls", "http://{0}:2379".format(my_address),
    "--listen-peer-urls", "http://{0}:2380".format(my_address),
    "--initial-advertise-peer-urls", "http://{0}:2380".format(my_address),
    "--initial-cluster-token", "my-cluster-token",
    "--initial-cluster-state", clusterstate,
    "--initial-cluster", clusterstring
    ]

print "Calling {0}".format(" ".join(etcd_args))
subprocess.call(etcd_args)