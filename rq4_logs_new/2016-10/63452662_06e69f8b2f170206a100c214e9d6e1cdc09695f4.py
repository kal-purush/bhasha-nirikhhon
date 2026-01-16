#!/usr/bin/env python
#
# This is a simple helper script to pre-create the job queue
#
import pika


connection = pika.BlockingConnection(pika.ConnectionParameters(
               'localhost'))
channel = connection.channel()

channel.queue_declare(queue='jobs.regular', durable=True)
