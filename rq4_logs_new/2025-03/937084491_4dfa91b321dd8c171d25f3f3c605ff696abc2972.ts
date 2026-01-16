import {
  DynamoDBClient,
  TransactWriteItemsCommand,
} from "@aws-sdk/client-dynamodb";
import { validateProduct } from "../shared/utils";
import { v4 as uuidv4 } from "/opt/nodejs/uuid-utils";
import { marshall } from "@aws-sdk/util-dynamodb";

const dynamoDbClient = new DynamoDBClient({ region: "eu-central-1" });

export const handler = async (event: any): Promise<any> => {
  console.log('Incoming event', event);
  let requestBody = event.body;
  let clientProductData;

  if (!requestBody) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: "Missing request body." }),
    };
  }

  try {
    clientProductData = JSON.parse(requestBody);
  } catch (err) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: "Invalid JSON Format." }),
    };
  }

  const productValidation = validateProduct(clientProductData);
  if (!productValidation.valid) {
    return {
      statusCode: 400,
      body: JSON.stringify(productValidation.message),
    };
  }

  const { title, description, price, count } = clientProductData;
  const newProductId = uuidv4();

  const product = { id: newProductId, title, description, price };
  const stock = { product_id: newProductId, count };

  const productTable = process.env.PRODUCT_TABLE;
  const stocksTable = process.env.STOCKS_TABLE;

  const params = {
    TransactItems: [
      {
        Put: {
          TableName: productTable,
          Item: marshall(product),
        },
      },
      {
        Put: {
          TableName: stocksTable,
          Item: marshall(stock),
        },
      },
    ],
  };

  try {
    const command = new TransactWriteItemsCommand(params);
    await dynamoDbClient.send(command);

    return {
      statusCode: 201,
      body: JSON.stringify({ message: "Product created successfully." }),
    };
  } catch (error) {
    console.error("Transaction failed:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Failed to create product", error }),
    };
  }
};