import { ProductDTO } from './../product.model';
import { APIGatewayProxyHandler } from 'aws-lambda';
import { defaultHeaders } from '../default-headers';
import { Client } from 'pg';
import { StatusCodes } from 'http-status-codes';
const { PG_HOST, PG_PORT, PG_DATABASE, PG_USERNAME, PG_PASSWORD } = process.env;
const dbOptions = {
  host: PG_HOST,
  port: +PG_PORT,
  database: PG_DATABASE,
  user: PG_USERNAME,
  password: PG_PASSWORD,
  ssl: {
    rejectUnauthorized: false // to avoid warning in this example
  },
  connectionTimeoutMillis: 5000 // time in millisecond for termination of the database query
};

export const createProduct: APIGatewayProxyHandler = async (event) => {
  console.log('Lambda invocation with event: ', event);

  const client = new Client(dbOptions);

  try {
    const product = JSON.parse(event.body) as ProductDTO;
    console.log('Product DTO: ', product);
    if (product && product.title && product.price) {
      return await createProductInDB(client, product);
    } else {
      return {
        statusCode: StatusCodes.BAD_REQUEST,
        body: JSON.stringify({ message: 'Invalid product' }),
        headers: defaultHeaders
      };
    }
  } catch (error) {
    return {
      statusCode: StatusCodes.INTERNAL_SERVER_ERROR,
      body: JSON.stringify({ message: error }),
      headers: defaultHeaders
    };
  }
};


async function createProductInDB(client: Client, product: ProductDTO) {
  await client.connect();
  await client.query(
    'INSERT INTO products (title, description, price) VALUES ($1, $2, $3)',
    [product.title, product.description, product.price]
  );
  return {
    statusCode: StatusCodes.OK,
    body: JSON.stringify({ message: 'Product created' }),
    headers: defaultHeaders
  };
}
