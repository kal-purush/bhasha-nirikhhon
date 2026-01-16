import { describe, expect, it } from '@jest/globals';
import { newClient, redisClient } from '../src/helpers/redis';
import { Redis } from 'ioredis';


const REDIS_URL = process.env['REDIS_URL'] || 'redis://redis.service.consul:6379';
const TEST_KEY = 'jest-test-key';
const TEST_VALUE = 'Gilbert, I\'m proud of you. That\'s good, Gilbert.';
describe('Redis connection tests', () => {
  it('Connects to redis and sets/gets some data', async () => {
    expect(redisClient).toBe(null);
    await newClient(REDIS_URL);
    expect(redisClient).toBeInstanceOf(Redis);

    await redisClient?.set(TEST_KEY, TEST_VALUE);
    expect(await redisClient?.get(TEST_KEY)).toBe(TEST_VALUE);
    await redisClient?.disconnect();
  }) 
})