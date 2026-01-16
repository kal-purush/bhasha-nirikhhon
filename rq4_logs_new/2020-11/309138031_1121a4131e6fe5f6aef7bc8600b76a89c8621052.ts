import { ProductDTO, Product } from '../product.model';
import { Client } from 'pg';
import { dbConfig } from '../pg.config';


export async function insertProduct(product: ProductDTO) {
  const client = new Client(dbConfig);

  try {
    await client.connect();
    await client.query('BEGIN');
    const insertResult = await client.query<Product>(
      'INSERT INTO products (title, description, price) VALUES ($1, $2, $3) RETURNING id',
      [product.title, product.description, product.price]
    );
    const insertedProductId = insertResult.rows?.[0]?.id;
    console.log('New product id:', insertedProductId);

    await client.query(
      'INSERT INTO stocks (product_id, count) VALUES ($1, $2)',
      [insertedProductId, +product.count]
    );
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.end();
  }
}