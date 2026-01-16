import { SQSEvent, SQSHandler } from 'aws-lambda';
import 'source-map-support/register';
import { Client, QueryResult } from "pg";
import { dbOptions } from '../utils/dbOptions';
import { Product } from '../models/productShema';

async function addProductToDB(client: Client, product: Product) {
  try {
    const { title, description, price, count } = product;

    await client.query("begin"); // transaction starts
    const insertedProductRes: QueryResult<Product> = await client.query(
      "insert into products (title, description, price) values ($1, $2, $3) returning *",
      [title, description, price]
    );
    const insertedProduct: Product = insertedProductRes.rows[0];

    await client.query(
      "insert into stocks (product_id, count) values ($1, $2)",
      [insertedProduct.id, count]
    );
    await client.query("commit"); // transaction ends
  } catch (err) {
    await client.query("rollback"); // cancel transaction
    console.error("Error while inserting product into DB", product, err);
  }
}

export const invoke: SQSHandler = async (event: SQSEvent) => {
  const client = new Client(dbOptions);
  await client.connect();

  for (let { body } of event.Records) {
    try {
      const { title, description, price, count } = JSON.parse(body);
      const product: Product = {
        title,
        description,
        price: price ? Number(price) : 0,
        count: count ? Number(count) : 0,
      };
      console.log({ product });

      await addProductToDB(client, product);
    } catch (err) {
      console.error(`Error while parsing product: ${body}`, err);
    }
  }

  // not use finally, because it closes client before other messages from queue received
  await client.end;
}