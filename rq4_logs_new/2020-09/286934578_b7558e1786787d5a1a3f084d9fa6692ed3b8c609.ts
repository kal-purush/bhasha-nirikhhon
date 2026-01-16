require("dotenv").config({ path: ".env" });
import { MongoClient } from "mongodb";

//dbURI empty variable
const { deploymentConnection } = process.env;
const lockalConnection: string = "mongodb://localhost:27017/test";
const dbConnection: string = deploymentConnection || lockalConnection;

const dbOptions = {
  useUnifiedTopology: true,
};

describe("insert", () => {
  let connection;
  let db;

  beforeAll(async () => {
    connection = await MongoClient.connect(dbConnection, dbOptions);
    db = await connection.db();
  });

  afterAll(async () => {
    await connection.close();
  });
});