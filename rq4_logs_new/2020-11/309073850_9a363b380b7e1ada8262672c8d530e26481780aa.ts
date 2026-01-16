import { APIGatewayProxyHandler } from 'aws-lambda';
import 'source-map-support/register';
import { Client } from "pg";
import { dbOptions } from './src/dbOptions';

export const invoke: APIGatewayProxyHandler = async event => {
  let client;
  try {
    const { title, description, price, count } = JSON.parse(event.body);
    client = new Client(dbOptions);
    await client.connect();

    await client.query("begin"); // transaction starts

    const insertedProductRes = await client.query(
      "insert into products (title, description, price) values ($1, $2, $3) returning *",
      [title, description, price]
    );

    await client.query(
      "insert into stocks (product_id, count) values ($1, $2)",
      [insertedProductRes.rows[0].id, count]
    );

    await client.query("commit"); // transaction ends

    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({
        ...insertedProductRes.rows[0],
        count,
      })
    }
  } catch (err) {
    await client.query("rollback"); // cancel transaction

    return {
      statusCode: 500,
      body: JSON.stringify({
        error: "Error during database request executing, Cannot insert product"
      })
    };
  } finally {
    // in case if error was occured, connection will not close automatically
    client?.end(); // manual closing of connection
  }
}