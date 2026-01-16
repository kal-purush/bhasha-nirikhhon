import { FastifyServerOptions } from 'fastify';
import { AppConfig } from './src/config';
import { join } from 'node:path';

export interface AppOptions extends FastifyServerOptions {}
const config = ({ logger }: AppConfig): AppOptions => {
  return {
    disableRequestLogging: true,
    requestIdLogLabel: 'reqId',
    requestIdHeader: 'x-request-id',
    genReqId: function (req) {
      return (
        (req.headers['x-server-request-id'] as string) || crypto.randomUUID()
      );
    },
    logger: {
      level: logger.logLevel || 'info',
      transport: {
        targets: [
          {
            target: 'pino/file',
            options: {
              destination: join(process.cwd(), logger.dir, 'app.log'),
            },
          },
          {
            target: logger.pretty ? 'pino-pretty' : 'pino/file',
            options: { destination: 1 },
          },
        ],
      },
      timestamp: () => {
        const dateString = new Date(Date.now()).toISOString();
        return `,"@timestamp":"${dateString}"`;
      },
      redact: {
        censor: '***',
        paths: [
          'req.headers.authorization',
          'req.body.password',
          'req.body.email',
        ],
      },
      serializers: {
        //@ts-ignore
        req: function (request) {
          const shouldLogBody = request.routeOptions.config?.logBody === true;
          return {
            method: request.method,
            url: request.raw.url,
            routeUrl: request.routeOptions.url,
            version: request.headers?.['accept-version'],
            user: request.user?.id,
            headers: {
              host: request.headers['host'],
              connection: request.headers['connection'],
              test: request.headers[':path'],
            },
            body: shouldLogBody ? request.body : undefined,
            hostname: request.hostname,
            remoteAddress: request.ip,
            remotePort: request.socket?.remotePort,
          };
        },
        res: function (reply) {
          return {
            statusCode: reply.statusCode,
            responseTime: reply.elapsedTime,
          };
        },
      },
    },
    ajv: {
      customOptions: {
        removeAdditional: 'all',
      },
    },
  };
};
export default (app: AppConfig) => config(app);