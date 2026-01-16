import { APIGatewayProxyHandler, APIGatewayProxyResult } from "aws-lambda";
import { Client } from "pg";
import { DB_OPTIONS } from "./options";

export const withPgConnection = (handler): APIGatewayProxyHandler => async (
  ...args
): Promise<APIGatewayProxyResult> => {
  const client = new Client(DB_OPTIONS);
  await client.connect();

  try {
    return await handler(client, ...args);
  } catch (err) {
    console.error("Error during database request executing:", err);
  } finally {
    client.end();
  }
};