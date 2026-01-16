import "source-map-support/register";
import { productsQuery } from "../../db/model/products";
import { withCorsHeaders } from "../../utils/withCorsHeaders";
import { withPgConnection } from "../../db/withPgConnection";
import { APIGatewayProxyHandler } from "aws-lambda";
import { stocksQuery } from "../../db/model/stocks";

const isProductValid = ({ title, price, count }) =>
  title &&
  count >= 0 &&
  Number.isInteger(count) &&
  price >= 0 &&
  Number.isInteger(price);

export const createProduct: APIGatewayProxyHandler = withPgConnection(
  async (client, event) => {
    const { title, description = "", price, src = "", count } = JSON.parse(
      event.body
    );

    if (!isProductValid({ title, price, count })) {
      return {
        statusCode: 400,
        body: JSON.stringify({ message: "Product is not valid" }),
      };
    }

    const {
      rows: [{ id }],
    } = await client.query(
      productsQuery.addRow({ title, description, price, src })
    );
    await client.query(stocksQuery.addRow({ id, count }));
    const {
      rows: [product],
    } = await client.query(productsQuery.getById(id));

    return withCorsHeaders({
      statusCode: 200,
      body: JSON.stringify(product),
    });
  }
);