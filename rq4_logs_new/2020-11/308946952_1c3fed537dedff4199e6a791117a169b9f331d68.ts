import { SQSEvent } from 'aws-lambda';
import * as AWSMock from 'aws-sdk-mock';

import { catalogBatchProcess } from './catalogBatchProcess';
import * as productsDb from './products-db';

jest.mock('./products-db');
AWSMock.mock('SNS', 'publish', 'test-message');

it('should create products', async () => {
  const products = [
    {
      title: 'Test 1',
      description: 'Description',
      count: 2,
      price: 20,
    },
    {
      title: 'Test 2',
      description: 'Description',
      count: 3,
      price: 10,
    },
  ];
  const event = ({
    Records: products.map((product) => ({
      body: JSON.stringify(product),
    })),
  } as unknown) as SQSEvent;

  (productsDb.validateProduct as any).mockResolvedValue(true);
  const createSpy = jest.spyOn(productsDb, 'createProduct'); 
  await catalogBatchProcess(event, null, null);
  expect(createSpy.mock.calls).toMatchSnapshot();
});