""" Kafka Consumer
Usage:
    consumer.py [--servers=<kn>] [--topics=<kn>]
    consumer.py --version
    consumer.py (-h | --help)

Options:
    -h --help       Show this screen
    --version       Show version
    --servers=<kn>  Servers separated by comma
    --topics=<kn>   Topics separated by comma
"""

from kafka import KafkaConsumer
from docopt import docopt


if __name__ == '__main__':
    arguments = docopt(__doc__, version='Kafka Consumer v0.1')

    servers = None
    if arguments.has_key('--servers'):
        servers = arguments.get('--servers')

    topics = []
    if arguments.has_key('--topics'):
        topics = arguments.get('--topics').split(',')

    consumer = KafkaConsumer(
        bootstrap_servers=servers
    )

    consumer.subscribe(topics)

    for msg in consumer:
        print msg