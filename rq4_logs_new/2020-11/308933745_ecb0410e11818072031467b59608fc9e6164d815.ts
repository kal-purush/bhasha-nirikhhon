import { APIGatewayProxyHandler } from 'aws-lambda';
import ProductService from '../services/product.service';

export const getProductById: APIGatewayProxyHandler = async (event, _context) => {
  const response = {
    statusCode: 200,
    body: ''
  };

  try {
    const { id } = event.queryStringParameters;
    const product = await ProductService.getProductById(id);
    response.body = JSON.stringify(product, null, 2);
  } catch (error) {
    console.log('ERROR', error);
    response.statusCode = 204;
    response.body = JSON.stringify(error, null, 2);
  }
  return response;
}