import { products } from '../products';
import { APIGatewayProxyHandler } from 'aws-lambda';

export const getProductById: APIGatewayProxyHandler = async (event) => {
  console.log('Lambda invocation with event: ', event);

  const { productId } = event.pathParameters;
  console.log('ID of requested product: ', productId);

  const product = products.find(p => p.id === +productId);
  if (product) {
    return {
      statusCode: 200,
      body: JSON.stringify(product)
    };
  };

  return {
    statusCode: 404,
    body: JSON.stringify({ message: 'Product not found' })
  };
};