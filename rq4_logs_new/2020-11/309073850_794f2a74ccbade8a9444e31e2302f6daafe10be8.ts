import { APIGatewayProxyHandler } from 'aws-lambda';
import 'source-map-support/register';
import { Client } from "pg";
import { dbOptions } from './src/dbOptions';

export const invoke: APIGatewayProxyHandler = async event => {
  const { productId } = event.pathParameters;
  let client;
  try {
    client = new Client(dbOptions);
    await client.connect();

    const { rows: products } = await client.query(
      `
        select p.id, s.count, p.price, p.title, p.description
        from products as p
        join stocks as s on p.id = s.product_id
        where p.id = $1
      `,
      [productId]
    );

    if (products?.length) {
      return {
        statusCode: 200,
        headers: {
          "Access-Control-Allow-Origin": "*",
        },
        body: JSON.stringify(products[0])
      };
    }
    return {
      statusCode: 404,
      headers: {
        "Access-Control-Allow-Origin": "*", //?
      },
      body: JSON.stringify({
        error: "Product not found"
      })
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Error during database request executing, Cannot get list of products"
      })
    };
  } finally {
    client?.end();
  }
}