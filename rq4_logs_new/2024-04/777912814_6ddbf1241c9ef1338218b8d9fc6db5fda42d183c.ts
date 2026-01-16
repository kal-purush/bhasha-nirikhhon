import { Handler, SQSEvent } from 'aws-lambda';
import logger from '../shared/logger';
import { EmailSender, Product as ProductService } from './services';
import { Product } from './types';

export const handler: Handler = async (event: SQSEvent) => {
  try {
    logger('catalogBatchProcess', event);

    const promises = event.Records.map(({ body }) => {
      const product: Product = JSON.parse(body);
      return ProductService.createOne(product);
    });

    const createdProducts = await Promise.all(promises);
    EmailSender.productCreated(createdProducts);

    return {
      body: 'OK',
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
      },
    };
  } catch (e) {
    return {
      body: JSON.stringify({
        error: e.name,
      }),
      statusCode: e.statusCode,
      headers: {
        'Access-Control-Allow-Origin': '*',
      },
    };
  }
};