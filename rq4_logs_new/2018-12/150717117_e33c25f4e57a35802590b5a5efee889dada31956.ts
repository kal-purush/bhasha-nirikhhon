import * as session from 'express-session'
import * as connectRedis from 'connect-redis'
import * as Redis from 'ioredis'

const { NODE_ENV, REDIS_ENDPOINT, REDIS_PW, SESSION_SECRET } = process.env

const REDIS_CONNECT =
  NODE_ENV === 'production'
    ? `redis://:${REDIS_PW}@${REDIS_ENDPOINT}`
    : ''

const RedisStore = connectRedis(session)
const redis = new Redis(REDIS_CONNECT)

export const sesh = session({
  store: new RedisStore({ client: redis as any }),
  name: 'qid',
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    maxAge: 1000 * 60 * 60 * 24 * 7 * 365
  }
})