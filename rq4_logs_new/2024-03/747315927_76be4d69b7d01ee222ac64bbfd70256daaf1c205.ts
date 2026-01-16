import { describe, test, expect } from '@jest/globals';
import request from 'supertest';
import app from '../../src/app';
import { prisma as prismaMock } from '../../prisma/client.js';
import prismaSpy from '../prismaSpy.test.js';
import authSpy from '../authSpy.test.js';

export interface testRouteParams {
  reqData: object;
  mockData: object;
  expectData: {
    status: number;
    body: object;
    calledWith: object;
  };
  model: string;
  id?: string;
  route?: string;
}

function generateController(model: string): string {
  if (model === 'baby') {
    model = 'babie';
  }
  return model + 's';
}

function generateURL(controller: string, id?: string, route?: string): string {
  if (controller && id && route) {
    return `/${controller}/:id/${route}`;
  }
  if (controller && id) {
    return `/${controller}/:id`;
  }
  if (controller && route) {
    return `/${controller}/${route}`;
  }
  return `/${controller}/all`;
}

// model should be the singular form
export function testRoute({
  reqData,
  mockData,
  expectData,
  model,
  id,
  route
}: testRouteParams): void {
  const controller = generateController(model);
  const url = generateURL(controller, id, route);
  describe(`Test ${url} route`, () => {
    test(`GET ${url} returns all ${controller}`, async () => {
      authSpy.requireAuth.mockImplementation(
        () =>
          (_req: object, _res: object, next: (...args: unknown[]) => unknown) =>
            next()
      );
      prismaSpy[controller][route].mockResolvedValue(mockData);
      const response = await request(app).get(url).send(reqData);
      expect(response.statusCode).toBe(expectData.status);
      expect(response.body).toEqual(expectData.body);
      expect(prismaMock[controller][route]).toHaveBeenCalledWith(
        expectData.calledWith
      );
    });
  });
}