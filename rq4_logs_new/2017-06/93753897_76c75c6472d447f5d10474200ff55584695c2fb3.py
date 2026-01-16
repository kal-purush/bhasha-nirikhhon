# Copyright (C) 2017 Eitan Geiger and Sebastian Scheinkman
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
import pika
from json import loads
from Jess.configs import grand_policy_loader
from Jess.plugin_loader import POLICY_LOADER
from Jess.Policy.firewall_rule import FirewallRule
from Jess.Policy.policy_optimizer import PolicyOptimizer

gpl = POLICY_LOADER[grand_policy_loader]
optimizer = PolicyOptimizer(gpl)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbit'))
channel = connection.channel()

channel.queue_declare(queue='policy_requests')
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    optimizer.add_rule(FirewallRule(**loads(body)))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=5)
channel.basic_consume(callback,
                      queue='policy_requests')

channel.start_consuming()