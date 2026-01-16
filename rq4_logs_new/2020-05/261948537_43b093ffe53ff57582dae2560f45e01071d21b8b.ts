import redis, { RedisClient } from 'redis'
import debug from 'debug'

const logger = debug('pnut-stream:redis')

function createRedisClient(): redis.RedisClient {
  const client = redis.createClient()
  return client
}

// singleton
function createRedisWrapper(client: RedisClient) {
  return () => ({
    add: registerUserToken(client),
    get: getUserTokens(client),
    remove: removeUserToken(client),
  })
}

export const redisClient = createRedisWrapper(createRedisClient())

function registerUserToken(client: redis.RedisClient) {
  return (id: string, token: string) => {
    client.rpush(id, token, callback)
  }
}

function removeUserToken(client: redis.RedisClient) {
  return (id: string, token: string) => {
    client.lrem(id, 0, token, callback)
  }
}

function getUserTokens(client: redis.RedisClient) {
  return (id: string) => {
    client.lrange(id, 0, -1, callback)
  }
}

function callback(err: Error | null) {
  if (err) return logger(err)
}