import { SQSHandler } from 'aws-lambda';
import { SNS } from 'aws-sdk';
import { Client } from 'pg';
import { REGION } from '../../constants';
import { dbConfig } from '../../db-config';
import {
  CREATE_PRODUCT_QUERY_STRING,
  CREATE_STOCK_QUERY_STRING,
  GET_PRODUCT_BY_ID_QUERY_STRING,
  INVALID_PRODUCT_DATA_MSG,
} from '../constants';
import { Product, ProductSchema } from '../models';

export const catalogBatchProcess: SQSHandler = async ({ Records }) => {
  console.log('Records: ', Records);
  const sns = new SNS({ region: REGION });
  let dbClient: Client;

  try {
    dbClient = new Client(dbConfig);
    await dbClient.connect();

    const createProductPromises = Records.map(async ({ body }) => {
      try {
        const isProductValid = await ProductSchema.isValid(JSON.parse(body));
        if (!isProductValid) {
          throw new Error(INVALID_PRODUCT_DATA_MSG);
        }
        const { title, description, price, count } = JSON.parse(body);

        await dbClient.query('BEGIN');

        const {
          rows: [{ id: newProductId }],
        } = await dbClient.query<Product>(CREATE_PRODUCT_QUERY_STRING, [
          title,
          description,
          price,
        ]);

        await dbClient.query<Product>(CREATE_STOCK_QUERY_STRING, [
          newProductId,
          count,
        ]);

        await dbClient.query<Product>(GET_PRODUCT_BY_ID_QUERY_STRING, [
          newProductId,
        ]);

        await dbClient.query('COMMIT');
      } catch (error) {
        if (dbClient) {
          await dbClient.query('ROLLBACK');
          throw new Error();
        }
      }
    });

    const createProductsResult = await Promise.allSettled(
      createProductPromises
    );
    const createdProducts = createProductsResult.filter(
      result => result.status === 'fulfilled'
    );
    const notCreatedProducts = createProductsResult.filter(
      result => result.status === 'rejected'
    );

    console.log('Publish using SNS');
    await sns
      .publish({
        Subject: 'Products created',
        Message: `Crated products: ${createdProducts.length},
        not created products: ${notCreatedProducts.length}`,
        TopicArn: process.env.SNS_ARN,
      })
      .promise();
  } catch ({ message }) {
    console.log(message);
  } finally {
    if (dbClient) {
      dbClient.end();
    }
  }
};