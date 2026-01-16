import { MongoClient } from "mongodb";
import { SERVER_ERROR } from "./errors";
let dbClient: MongoClient;
export async function initDbClient() {
  try {
    dbClient = await MongoClient.connect((process.env.DB_URI as string) || "", {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      ignoreUndefined: true,
    });
    return dbClient;
  } catch (error) {
    throw error;
  }
}

export async function getDbClient() {
  if (!dbClient) {
    await initDbClient();
  }
  return dbClient;
}