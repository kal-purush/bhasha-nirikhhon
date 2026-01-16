""" Copyright start
  Copyright (C) 2008 - 2022 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """

from connectors.core.connector import Connector, get_logger, ConnectorError
from .operations import check_health, operations

logger = get_logger('twitter-feed')

class TwitterFeed(Connector):
    def execute(self, config, operation, params, **kwargs):
        try:
            logger.info('In execute() Operation:[{}]'.format(operation))
            operation = operations.get(operation, None)
            if not operation:
                logger.info('Unsupported operation [{}]'.format(operation))
                raise ConnectorError('Unsupported operation')
            result = operation(config, params)
            return result
        except Exception as err:
            logger.exception(err)
            raise ConnectorError(err)

    def check_health(self, config):
        try:
            return check_health(config)
        except Exception as err:
            logger.exception(err)
            raise ConnectorError(err)