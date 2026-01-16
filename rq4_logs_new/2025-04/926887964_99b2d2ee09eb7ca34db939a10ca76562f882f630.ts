// For Pact testing with Vitest
import { Pact, Matchers } from '@pact-foundation/pact';
import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  afterEach,
  vi,
} from 'vitest';
import { fetchOrder, createOrder } from '../api';
import { OrderCreate } from '../../types';
import * as path from 'path';

// Must be before any constants declarations due to hoisting
vi.mock('../../config', () => ({
  config: {
    apiUrl: 'http://localhost:9777',
  },
}));

const PACT_PORT = 9777;
// Get the absolute path to the project root
const PROJECT_ROOT = path.resolve(process.cwd());

describe('Orders API Error Scenarios Pact', () => {
  const provider = new Pact({
    consumer: 'wms_ui',
    provider: 'wms_order_management',
    port: PACT_PORT,
    log: path.join(PROJECT_ROOT, 'logs', 'order-error-pact.log'),
    dir: path.join(PROJECT_ROOT, 'pacts'),
    pactfileWriteMode: 'merge',
    logLevel: 'warn',
  });

  beforeAll(async () => {
    await provider.setup();
  });

  afterAll(async () => {
    await provider.finalize();
  });

  afterEach(async () => {
    await provider.verify();
  });

  describe('get order by id - not found', () => {
    it('returns a 404 when order does not exist', async () => {
      const nonExistentOrderId = 9999;

      // Arrange - Setup the expected interaction
      await provider.addInteraction({
        state: `order with ID ${nonExistentOrderId} does not exist`,
        uponReceiving: `a request for non-existent order with ID ${nonExistentOrderId}`,
        withRequest: {
          method: 'GET',
          path: `/api/v1/orders/${nonExistentOrderId}`,
        },
        willRespondWith: {
          status: 404,
          headers: {
            'Content-Type': 'application/json',
          },
          body: {
            error: Matchers.string('Order not found'),
          },
        },
      });

      // Act & Assert - Expect the request to throw an error
      await expect(fetchOrder(nonExistentOrderId)).rejects.toThrow('API error: 404');
    });
  });

  describe('create order - validation error', () => {
    it('returns a 400 with validation errors when order data is invalid', async () => {
      // Invalid order data - missing required fields
      const invalidOrderData: OrderCreate = {
        customerId: 0, // Invalid customer ID
        items: [], // Empty items array
      };

      // Arrange - Setup the expected interaction
      await provider.addInteraction({
        state: 'order validation will fail',
        uponReceiving: 'a request to create an invalid order',
        withRequest: {
          method: 'POST',
          path: '/api/v1/orders',
          headers: {
            'Content-Type': 'application/json',
          },
          body: invalidOrderData,
        },
        willRespondWith: {
          status: 400,
          headers: {
            'Content-Type': 'application/json',
          },
          body: {
            error: Matchers.string('Validation error'),
            details: Matchers.eachLike({
              field: Matchers.string('customerId'),
              message: Matchers.string('Must be a positive integer'),
            }),
          },
        },
      });

      // Act & Assert - Expect the request to throw an error
      await expect(createOrder(invalidOrderData)).rejects.toThrow('API error: 400');
    });
  });

  describe('create order - product not found', () => {
    it('returns a 404 when referenced product does not exist', async () => {
      // Order with non-existent product
      const orderWithNonExistentProduct: OrderCreate = {
        customerId: 1,
        items: [
          {
            productId: 9999, // Non-existent product ID
            quantity: 1,
          },
        ],
      };

      // Arrange - Setup the expected interaction
      await provider.addInteraction({
        state: 'product with ID 9999 does not exist',
        uponReceiving: 'a request to create an order with non-existent product',
        withRequest: {
          method: 'POST',
          path: '/api/v1/orders',
          headers: {
            'Content-Type': 'application/json',
          },
          body: orderWithNonExistentProduct,
        },
        willRespondWith: {
          status: 404,
          headers: {
            'Content-Type': 'application/json',
          },
          body: {
            error: Matchers.string('Product not found'),
            productId: Matchers.integer(9999),
          },
        },
      });

      // Act & Assert - Expect the request to throw an error
      await expect(createOrder(orderWithNonExistentProduct)).rejects.toThrow('API error: 404');
    });
  });

  describe('api server error', () => {
    it('handles server errors gracefully', async () => {
      // Arrange - Setup the expected interaction
      await provider.addInteraction({
        state: 'server is experiencing issues',
        uponReceiving: 'a request during server errors',
        withRequest: {
          method: 'GET',
          path: '/api/v1/orders',
        },
        willRespondWith: {
          status: 500,
          headers: {
            'Content-Type': 'application/json',
          },
          body: {
            error: Matchers.string('Internal server error'),
          },
        },
      });

      // We're testing the fetchOrders function but using the import alias to avoid conflicts
      const { fetchOrders } = await import('../api');
      
      // Act & Assert - Expect the request to throw an error
      await expect(fetchOrders()).rejects.toThrow('API error: 500');
    });
  });
}); 