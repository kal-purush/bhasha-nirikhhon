import { APIGatewayProxyHandler } from 'aws-lambda';
import 'source-map-support/register';

import { createProduct, ProductEntity } from './products-db';

function validateProduct(product: ProductEntity) {
  return (typeof product.title === 'string' && product.title.length > 0) &&
    (!product.description || typeof product.description === 'string') &&
    (!product.price || typeof product.price === 'number');
}

const headers = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, PATCH, PUT',
};

export const postProduct: APIGatewayProxyHandler = async (event, _context) => {
  try {
    console.log(event.body);
    const product = JSON.parse(event.body || '');
    if (!product || !(product instanceof Object) || !validateProduct(product)) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          message: 'Bad request',
        }, null, 2),
        headers,
      };
    }
    const result = await createProduct(product);
    return {
      statusCode: 200,
      body: JSON.stringify({
        product: result,
      }, null, 2),
      headers,
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify(err.message || err.details || err, null, 2),
      headers,
    };
  }
}