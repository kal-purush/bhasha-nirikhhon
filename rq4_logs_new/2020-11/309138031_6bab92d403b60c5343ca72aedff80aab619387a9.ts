import { getProductById } from './get-product-by-id';
import { products } from '../products';

describe('getProductById', () => {
  describe('if product exists', () => {
    it('should return response with code 200 and correct product', async () => {
      const fakeEvent = {
        pathParameters: { productId: `${products[0].id}` }
      };
      const result = await getProductById(fakeEvent as any, null, null);

      expect(result).toEqual({
        statusCode: 200,
        body: JSON.stringify(products[0]),
        headers: {
          'Access-Control-Allow-Origin': '*'
        }
      });
    });
  });

  describe('if product does not exist', () => {
    it('should return response with code 404 and error message', async () => {
      const fakeEvent = {
        pathParameters: { productId: '-1' }
      };
      const result = await getProductById(fakeEvent as any, null, null);

      expect(result).toEqual({
        statusCode: 404,
        body: JSON.stringify({ message: 'Product not found' }),
        headers: {
          'Access-Control-Allow-Origin': '*'
        }
      });
    });
  });
});