import session from "fastify-session";
import { Redis } from "ioredis";

type RedisStoreOptions = {
  client: Redis;
  prefix?: string;
  ttl?: number;
};

type SessionData = Record<string, unknown>;

export class RedisStore<T extends SessionData> implements session.SessionStore {
  private redis: Redis;
  private readonly prefix: string;
  private readonly ttl: number;

  constructor({ client, prefix = "sess:", ttl = 86_400 }: RedisStoreOptions) {
    this.redis = client;
    this.prefix = prefix;
    this.ttl = ttl;
  }

  private getKey(sessionId: string) {
    return `${this.prefix}${sessionId}`;
  }

  async get(
    sessionId: string,
    callback: (err?: Error, session?: T) => void
  ): Promise<void> {
    const key = this.getKey(sessionId);
    try {
      const value = await this.redis.hgetall(key);
      callback(undefined, JSON.parse(value.data ?? "{}"));
    } catch (e) {
      callback(e);
    }
  }

  async set(
    sessionId: string,
    session: T,
    callback: (err?: Error) => void
  ): Promise<void> {
    const key = this.getKey(sessionId);
    const data = JSON.stringify(session);
    try {
      await this.redis
        .pipeline()
        .hset(key, "data", data)
        .expire(key, this.ttl)
        .exec();
      callback();
    } catch (e) {
      callback(e);
    }
  }

  async destroy(
    sessionId: string,
    callback: (err?: Error) => void
  ): Promise<void> {
    const key = this.getKey(sessionId);
    try {
      await this.redis.del(key);
      callback();
    } catch (e) {
      callback(e);
    }
  }
}