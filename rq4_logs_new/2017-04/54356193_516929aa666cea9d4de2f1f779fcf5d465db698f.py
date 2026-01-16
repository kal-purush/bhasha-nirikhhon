#!/usr/bin/python

import random
import re
import sys
import time

from cassandra.cluster import Cluster
import paramiko

"""
To run this script, execute:
$ python test_cassandra_ha.py <data_amount>

Parameter <data_amount> is the number of data that you want to write to Cassandra.
Set to greater than 100 will trigger node-down job, to test HA.
"""

#################################
# Here are your configurations. #
#################################
CLUSTER_ADDRESSES = ["192.168.122.122",
                     "192.168.122.123",
                     "192.168.122.124",
#                     "192.168.122.125",
                     "192.168.122.126"]

SEED_NODES = ["192.168.122.122", "192.168.122.123"]

SCHEMA_SCRIPT = "drop table if exists bigboy.info;" \
                "drop schema if exists bigboy;" \
                "create schema bigboy" \
                "    with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };" \
                "use bigboy;" \
                "create table bigboy.info(" \
                "    boy_id text," \
                "    time_stamp timestamp," \
                "    primary key (boy_id, time_stamp)" \
                ");"
KEYSPACE = "bigboy"
TABLE = "bigboy.info"
REPLICATION_FACTOR = 3
INSERT_INTERVAL = 0
INSERT_AMOUNT = int(sys.argv[1])
#################################
#################################


def get_ring_distribute(host, username="root", password="password"):

    """
    cmd ` nodetool describering <keyspace> ` will output:
    TokenRange(start_token:5940577727476728623, end_token:5943584674441571405, endpoints:[192.168.122.111], rpc_endpoints:[192.168.122.111], endpoint_details:[EndpointDetails(host:192.168.122.111, datacenter:datacenter1, rack:rack1)])\n'

    we want to convert it to a list of (start_token, end_token, endpoints) pairs, such as:
    [(-78030903380813158, -74533601447532126, "192.168.122.113"),
     (4218276711791353687, 4219841154467858666, "192.168.122.111"),
     ...
    ]
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)

    stdin, stdout, stderr = ssh.exec_command("nodetool describering %s" % KEYSPACE)
    ring = "".join(stdout.readlines())
    ssh.close()

    # Assemble start tokens:  
    start_token_head_list = [m.end() for m in re.finditer("start_token:", ring)]
    start_token_tail_list = [m.start() for m in re.finditer(', end_token', ring)]
    start_token_list = []
    for start_token_head, start_token_tail in zip(start_token_head_list, start_token_tail_list):
        start_token_list.append(int(ring[start_token_head : start_token_tail]))

    # Assemble end tokens:
    end_token_head_list = [m.end() for m in re.finditer('end_token:', ring)]
    end_token_tail_list = [m.start() for m in re.finditer(', endpoints', ring)]
    end_token_list = []
    for end_token_head, end_token_tail in zip(end_token_head_list, end_token_tail_list):
        end_token_list.append(int(ring[end_token_head : end_token_tail]))

    # Assemble endpoints:
    endpoints_head_list = [m.end() for m in re.finditer(' endpoints:\[', ring)]
    endpoints_tail_list = [m.start() for m in re.finditer('\], rpc_endpoints', ring)]
    endpoints_list = []
    for endpoints_head, endpoints_tail in zip(endpoints_head_list, endpoints_tail_list):
        endpoints_list.append(ring[endpoints_head : endpoints_tail])

    return zip(start_token_list, end_token_list, endpoints_list)

def start_cassandra_service(host, username="root", password="password"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)

    ssh.exec_command("service cassandra start")
    ssh.close()

def stop_cassandra_service(host, username="root", password="password"):
    isCassandraStopped = False

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)

    ssh.exec_command("service cassandra stop")
    time.sleep(10)

    _, stdout, _ = ssh.exec_command("service cassandra status")
    if 'Active: inactive' in ''.join(stdout.readlines()):
        isCassandraStopped = True

    ssh.close()

    return isCassandraStopped

def main():
    # Step1: Connect to Cassandra server.
    try:
        cluster = Cluster(CLUSTER_ADDRESSES)
        session = cluster.connect()
    except:
        print "Error to connect to Cassandra server."
        exit(1)

    # Step2: Create schema for test.
    try:
        for script in SCHEMA_SCRIPT.split(';'):
            if script:
                session.execute(script)
        time.sleep(2)
    except Exception as e:
        print e
        print "Error occurs during creating schema for test."
        exit(1)

    # Step3: Get ring partition details for schema above..
    for host in CLUSTER_ADDRESSES:
        try:
            ring_distribute = get_ring_distribute(host)
            break
        except:
            print "Error connecting to %s to get ring info." % host

    ring_collect = {}
    down_nodes_number = REPLICATION_FACTOR - 1
    seed_node_list = SEED_NODES
    stor_node_list = [node for node in CLUSTER_ADDRESSES if node not in SEED_NODES]
    down_nodes = []

    # Step4: Start to insert data.
    # TODO: Enhance to asynchrounous style such as using threadpool.
    for idx in range(INSERT_AMOUNT):
        # 4-1: Write data into database.
        session.execute("INSERT INTO %s (boy_id, time_stamp) VALUES ('%s', %s)" % (TABLE, idx, int(time.time())))
        time.sleep(0.05)
        token_obj = session.execute("SELECT token(boy_id) from %s where boy_id='%s'" % (TABLE, idx))
        token = token_obj._current_rows[0].system_token_boy_id

        # 4-2: To know which node data is written to.
        for head, tail, endpoints in ring_distribute:
            if tail <= token <= head:
                print " Write data %s to %s ..." % (idx, endpoints)

                # endpoints might be "123.123.123.123" or "123.123.123.123, 123.123.123.124"
                # or "123.123.123.123, 123.123.123.124, 123.123.123.125"
                for ep in endpoints.split(', '):
                    if ep not in ring_collect:
                        ring_collect[ep] = 1
                    else:
                        ring_collect[ep] += 1

                break

        # 4-3: Stop Cassandra on some nodes but lower priority for stopping onseed nodes.
        if down_nodes_number > 0 and INSERT_AMOUNT > 100 and idx == INSERT_AMOUNT/(down_nodes_number + 1):
            if stor_node_list:
                down_node = random.choice(stor_node_list)
            else:
                down_node = random.choice(seed_node_list)

            print "#####################################################################"
            print " Plan to stop Cassandra service on node: %s ..." % down_node
            isCassandraStopped = stop_cassandra_service(down_node)
            if isCassandraStopped:
                down_nodes.append(down_node)
                if down_node in stor_node_list:
                    stor_node_list.remove(down_node)
                else:
                    seed_node_list.remove(down_node)
                down_nodes_number -= 1
                print "#####################################################################"
                print " Cassandra service is stopped on node: %s" % down_node
                print " Cassandra service DOWN: %s" % down_nodes
                print " Cassandra service UP: %s" % (stor_node_list + seed_node_list)
                print "#####################################################################\n"
                time.sleep(2)

        if "INSERT_INTERVAL" in globals():
            time.sleep(INSERT_INTERVAL)

    # Step5: Restart Cassandra service..
    for node in down_nodes:
        start_cassandra_service(node)

    # Print result.
    print "******************************************************************"
    print " Amount of queries: %s. Count of data on each node: \n" % INSERT_AMOUNT
    for endpoint in ring_collect:
        print "     %s: %s \n" % (endpoint, ring_collect[endpoint])

if __name__ == "__main__":
    main()