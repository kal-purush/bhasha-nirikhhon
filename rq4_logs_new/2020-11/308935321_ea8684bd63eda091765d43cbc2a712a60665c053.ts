import { APIGatewayProxyEvent } from 'aws-lambda';
import axios from 'axios';

import { productsMock } from '../mocks';
import { getProductById } from './get-product-by-id';

jest.mock('axios');
const axiosMock = axios as jest.Mocked<typeof axios>;

describe('getProductById', () => {
  axiosMock.get.mockResolvedValue({ data: productsMock });

  test('should return product if it exists', async () => {
    const product = productsMock[0];
    const expectedResult = {
      headers: { 'Access-Control-Allow-Origin': '*' },
      statusCode: 200,
      body: JSON.stringify(product),
    };

    expect(
      await getProductById(
        ({
          pathParameters: { productId: product.id },
        } as unknown) as APIGatewayProxyEvent,
        null,
        null
      )
    ).toEqual(expectedResult);
  });

  test("should return not-found message if product doesn't exist", async () => {
    const notExistingProductId = 'product-id';
    const expectedResult = {
      headers: { 'Access-Control-Allow-Origin': '*' },
      statusCode: 404,
      body: 'Product not found',
    };

    expect(
      await getProductById(
        ({
          pathParameters: { productId: notExistingProductId },
        } as unknown) as APIGatewayProxyEvent,
        null,
        null
      )
    ).toEqual(expectedResult);
  });
});