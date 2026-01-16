import { SNS } from "aws-sdk";
import { productsQuery } from "../../db/model/products";
import { stocksQuery } from "../../db/model/stocks";
import { withPgConnection } from "../../db/withPgConnection";

const { REGION, SNS_ARN } = process.env;

export const catalogBatchProcess = withPgConnection(async (client, event) => {
  const sns = new SNS({ region: REGION });
  const products = event.Records.map(({ body }) => JSON.parse(body));

  try {
    await Promise.all(
      products.map((product) => addProductToDb(client, product))
    );

    await sns
      .publish(
        {
          Subject: "New products announcement",
          Message: JSON.stringify(products),
          TopicArn: SNS_ARN,
        },
        (error, data) => {
          if (error) {
            console.log("SNS error: ", error);
            return;
          }

          console.log("Products were sent to SNS:", data);
        }
      )
      .promise();
  } catch (error) {
    console.log("Error", error?.message || "Something went wrong");
  }
});

const addProductToDb = async (client, product) => {
  const { title, price, count, description, src } = formatProduct(product);

  if (!isProductValid({ title, price, count })) {
    throw new Error("Product is not valid");
  }

  const {
    rows: [{ id }],
  } = await client.query(productsQuery.addRow, [
    title,
    description,
    price,
    src,
  ]);
  await client.query(stocksQuery.addRow, [id, count]);

  console.log(`Product ${title} was created`);
};

const formatProduct = ({
  title,
  price,
  count,
  description = "",
  src = "",
}) => ({
  title,
  price: parseInt(price, 10),
  count: parseInt(count, 10),
  description,
  src,
});

const isProductValid = ({ title, price, count }) =>
  title &&
  count >= 0 &&
  Number.isInteger(count) &&
  price >= 0 &&
  Number.isInteger(price);