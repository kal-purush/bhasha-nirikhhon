#!/usr/bin/env python3

import os
import socket

# Define the number of ports to publish for each sandbox instance.
num_port_to_publish = 3


# get_unused_port returns an unused port
def get_unused_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port

ports_to_publish = sorted([
    get_unused_port() 
    for _ in range(num_port_to_publish)
])

docker_publish_args = []
for p in ports_to_publish:
    docker_publish_args.append('--publish')
    docker_publish_args.append('{port}:{port}'.format(port=p))

# Replace current process with Docker command
os.execvp(
    'docker',
    [
        'docker', 'run',
        '--interactive',
        '--tty',
        '--label', 'docker-sandbox',
        '--privileged',
    ] + docker_publish_args + [
        '--env', 'PUBLISHED_PORTS=' + ' '.join([str(p) for p in ports_to_publish]),
        'docker-sandbox',
    ]
)