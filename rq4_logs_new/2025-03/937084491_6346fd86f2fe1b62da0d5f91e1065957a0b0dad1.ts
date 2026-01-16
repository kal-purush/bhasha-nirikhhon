import {
  DynamoDBClient,
  TransactWriteItemsCommand,
} from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from "/opt/nodejs/uuid-utils";
import { marshall } from "@aws-sdk/util-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const client = new DynamoDBClient({ region: "eu-central-1" });
const documentClient = DynamoDBDocumentClient.from(client);
const snsClient = new SNSClient({ region: "eu-central-1" });

export const handler = async (event: any) => {
  console.log("Incoming event:", event);
  const productTable = process.env.PRODUCT_TABLE!;
  const stocksTable = process.env.STOCKS_TABLE!;
  const topicArn = process.env.CREATE_PRODUCT_TOPIC_ARN!;

  for (const record of event.Records) {
    try {
      const data = JSON.parse(record.body);
      const id = uuidv4();

      const productItem = {
        id,
        title: data.title,
        description: data.description,
        price: Number(data.price),
      };

      const stockItem = {
        product_id: id,
        count: data.count !== undefined ? Number(data.count) : 0,
      };

      const params = {
        TransactItems: [
          {
            Put: {
              TableName: productTable,
              Item: marshall(productItem),
            },
          },
          {
            Put: {
              TableName: stocksTable,
              Item: marshall(stockItem),
            },
          },
        ],
      };

      const command = new TransactWriteItemsCommand(params);
      await documentClient.send(command);

      console.log(
        "Inserted product and stock into DynamoDB:",
        productItem,
        stockItem
      );

      const messageAttributes = {
        price: {
          DataType: "Number",
          StringValue: String(productItem.price),
        },
      };

      const publishCommand = new PublishCommand({
        TopicArn: topicArn,
        Message: JSON.stringify({
          message: "Product created",
          product: productItem,
        }),
        MessageAttributes: messageAttributes,
      });

      await snsClient.send(publishCommand);
      console.log("Published product creation event to SNS:", topicArn);
      
    } catch (error) {
      console.error("Error processing record:", record, error);
    }
  }
};