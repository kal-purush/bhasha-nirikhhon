import pino, { Logger } from 'pino';
import { ecsFormat } from '@elastic/ecs-pino-format';

const logLevel = process.env.LOG_LEVEL || 'info';

/*
 * Logger using Pino and the Elastic Common Schema format to facilitate
 * standardization across all log messaging. This format will allow us
 * to more easily debug and track requests in OpenSearch.
 */
export const logger: Logger = pino({ level: logLevel,  ...ecsFormat })

// Add the additional args to the ECS logger so that they are queryable fields.
// At least you should send the sessionId if available and the error (if applicable)
export function formatLogMessage(logger: Logger, args: Object | void): Logger {
  if (args) {
    // Add the args to the ECS logger
    return logger.child({ ...args });
  }
  // Otherwise there were no additional arfs, so return the logger as-is
  return logger;
}