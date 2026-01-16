import { jest, describe, test, expect } from '@jest/globals';
import request from 'supertest';

// unstable_mockModule for esm module mocking compatibility
jest.unstable_mockModule('../../src/services/auth.js', () => ({
  auth: {
    requireAuth: jest
      .fn()
      .mockImplementation(
        (_req: unknown, _res: unknown, next: (...args: unknown[]) => unknown) =>
          next()
      )
  }
}));

// use dynamic import after for oauth mocks to work
const app = (await import('../../src/app.js')).default;

describe(`Test /users/all integration`, () => {
  test(`GET /users/all`, async () => {
    const response = await request(app).get('/users/all');
    console.log(JSON.stringify(response));
    expect(response.statusCode).toBe(200);
  });
});