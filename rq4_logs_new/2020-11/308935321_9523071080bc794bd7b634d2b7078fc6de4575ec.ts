import { SQSRecord } from 'aws-lambda';
import * as AWSMock from 'aws-sdk-mock';
import { Product } from '../models';

import { catalogBatchProcess } from './catalog-batch-process';

jest.mock('pg', () => {
  const mockClient = {
    connect: jest.fn().mockImplementation(null),
    query: jest.fn().mockResolvedValue({ rows: [{ id: '1' }] }),
    end: jest.fn().mockResolvedValue(null),
  };
  return { Client: jest.fn(() => mockClient) };
});

describe('catalogBatchProcess', () => {
  it('should create correcr message', async () => {
    let message = '';
    AWSMock.mock('SNS', 'publish', (params, cb) => {
      message = params.Message;
      cb();
    });
    const mockValidProduct: Omit<Product, 'id'> = {
      title: 'title',
      description: 'description',
      price: 100,
      count: 2,
    };
    const mockInvalidProduct = {
      title: 'title',
      description: 'description',
      price: 100,
    };

    await catalogBatchProcess(
      {
        Records: [
          {
            body: JSON.stringify(mockValidProduct),
          } as SQSRecord,
          {
            body: JSON.stringify(mockInvalidProduct),
          } as SQSRecord,
        ],
      },
      null,
      null
    );
    expect(message).toBe(`Crated products: 1, not created products: 1`);
  });
});