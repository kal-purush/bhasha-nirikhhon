import morgan from 'morgan'
import { StorefrontApiContext } from '@storefront-api/lib/module/types'

export default function registerLogging ({ app: express, logger }: StorefrontApiContext): void {
  // GAE metrics
  morgan.token('gae-instance-id', () => process.env.GAE_INSTANCE || '-')
  morgan.token('gae-version', () => process.env.GAE_VERSION || '-')
  morgan.token('gae-service', () => process.env.GAE_SERVICE || '-')
  morgan.token('gae-severity', (req, res) => res.statusCode < 400 ? 'INFO' : 'ERROR')

  // API metrics
  morgan.token('cache', (req, res) => res.getHeader('x-vs-cache') || 'cache-none')
  morgan.token('body', (req, res) => (res.statusCode >= 400 || req.method === 'POST') ? req.body : '')
  morgan.token('response-body', (req, res) => res.statusCode >= 400 ? res.body : '')

  if (process.env.GCLOUD_OPERATIONS_ENABLED) {
    // GAE structured-data output
    const morganStream = morgan((tokens, req, res) => {
      const payload = {
        severity: tokens['gae-severity'](req, res),
        'logging.googleapis.com/labels': {
          module_id: tokens['gae-service'](req, res),
          version_id: tokens['gae-version'](req, res),
          instance_id: tokens['gae-instance-id'](req, res)
        },
        status: tokens.status(req, res),
        method: tokens.method(req, res),
        url: tokens.url(req, res),
        body: tokens.body(req, res),
        response: tokens['response-body'](req, res),
        contentLength: tokens.res(req, res, 'content-length'),
        responseTime: tokens['response-time'](req, res) + 'ms',
        cache: tokens.cache(req, res)
      }

      return JSON.stringify(payload)
    })

    express.use(morganStream)
  } else {
    // Default output
    express.use(morgan(':method :url :status :res[content-length] :cache :gae-instance-id - :response-time ms'))
  }

  logger.info('Initialized "morgan" for stdout request logging')
}