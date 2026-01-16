"""Helper functions for generating confluent kafka configuration."""

import os

# For more configuration options go to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
# and add them to these dictionaries

confluent_config = {
    "bootstrap.servers": ("KAFKA_BROKER_URL", str),
    "group.id": ("KAFKA_CONSUMER_GROUP_ID", str),
    "client.id": ("KAFKA_CLIENT_ID", str),
    "message.max.bytes": ("KAFKA_MESSAGE_MAX_BYTES", int),
    "receive.message.max.bytes": ("KAFKA_RECEIVE_MESSAGE_MAX_BYTES", int),
    "security.protocol": ("KAFKA_SECURITY_PROTOCOL", str),
    "ssl.certificate.location": ("KAFKA_SSL_CERTIFICATE_LOCATION", str),
    "ssl.ca.location": ("KAFKA_SSL_CA_LOCATION", str),
    "ssl.endpoint.identification.algorithm": ("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", str),
    "ssl.key.location": ("KAFKA_SSL_KEY_LOCATION", str),
    "ssl.key.password": ("KAFKA_SSL_KEY_PASSWORD", str),
    "sasl.mechanism": ("KAFKA_SASL_MECHANISM", str),
    "sasl.username": ("KAFKA_SASL_USERNAME", str),
    "sasl.password": ("KAFKA_SASL_PASSWORD", str),
    "group.instance.id": ("KAFKA_CONSUMER_GROUP_INSTANCE_ID", str),
    "max.poll.interval.ms": ("KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS", float),
    "enable.auto.commit": ("KAFKA_CONSUMER_ENABLE_AUTO_COMMIT", bool),
    "auto.offset.reset": ("KAFKA_CONSUMER_AUTO_OFFSET_RESET", str),
}


def kafka_config_from_env():
    """ Generate Confluent Kafka configuration from environment variables. """
    config = {}
    for key, (env_key, type_cast) in confluent_config.items():
        if env_key in os.environ:
            config[key] = type_cast(os.environ[env_key])
    return config