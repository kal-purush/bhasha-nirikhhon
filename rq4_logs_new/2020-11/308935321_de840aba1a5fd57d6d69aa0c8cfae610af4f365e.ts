import { APIGatewayProxyHandler } from 'aws-lambda';
import 'source-map-support/register';

import { Product } from '../models';
import { Client } from 'pg';
import { dbConfig } from '../../db-config';
import { createResponse } from '../../helpers';
import { getProductByIdqueryString } from './get-product-by-id';

const createProductQueryString =
  'INSERT INTO products(title, description, price) VALUES ($1, $2, $3) RETURNING id';
const createStocktQueryString =
  'INSERT INTO stocks(product_id, count) VALUES ($1, $2)';
const invalidProductDataMsg = 'Product data is invalid';

export const createProduct: APIGatewayProxyHandler = async ({ body }) => {
  console.log('body: ', body);
  let dbClient: Client;

  try {
    if (isInvalidProductData(JSON.parse(body))) {
      throw new Error(invalidProductDataMsg);
    }
    const { title, description, price, count } = JSON.parse(body);
    dbClient = new Client(dbConfig);
    await dbClient.connect();
    await dbClient.query('BEGIN');

    const {
      rows: [{ id: newProductId }],
    } = await dbClient.query<Product>(createProductQueryString, [
      title,
      description,
      price,
    ]);

    await dbClient.query<Product>(createStocktQueryString, [
      newProductId,
      count,
    ]);

    const {
      rows: [product],
    } = await dbClient.query<Product>(getProductByIdqueryString, [
      newProductId,
    ]);

    await dbClient.query('COMMIT');

    return createResponse(201, product);
  } catch ({ message }) {
    await dbClient.query('ROLLBACK');

    if (message === invalidProductDataMsg) {
      return createResponse(400, { message });
    }

    return createResponse(500, { message: 'Internal Server Error' });
  } finally {
    if (dbClient) {
      dbClient.end();
    }
  }
};

function isInvalidProductData({
  title,
  description,
  price,
  count,
}: Product): boolean {
  return (
    !isValidValue(title, 'string') ||
    !isValidValue(description, 'string') ||
    !isValidValue(price, 'number') ||
    !isValidValue(count, 'number')
  );
}

function isValidValue(value: any, type: string): boolean {
  return !!value && typeof value === type;
}