import config from 'config'
import logger from './util/logger.js'

let modulesCache

export async function handleModules (content, source, urlSetConf, urlDataId) {
  if (modulesCache) {
    logger.info('Load modules cache')
    for (const mod in modulesCache) {
      try {
        const module = modulesCache[mod]
        module.handle(content, source, urlSetConf, urlDataId)
      } catch (error) {
        logger.error(error, mod + ' handling failed')
      }
    }
  } else {
    logger.warn('Init modules')
    modulesCache = {}
    for (const mod in config.modules) {
      logger.info(`Found module: ${mod}`)
      try {
        const module = await import(`../modules/${mod}.js`)
        module.handle(content, source, urlSetConf, urlDataId)
        modulesCache[mod] = module
      } catch (error) {
        logger.error(error, mod + ' handling failed')
      }
    }
    logger.info('Finish modules handling')
  }
}