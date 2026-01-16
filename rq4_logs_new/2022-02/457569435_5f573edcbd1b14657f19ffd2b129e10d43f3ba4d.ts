require("dotenv").config();
import { Client } from 'pg';
const dbClient = new Client();
dbClient.connect();

export { dbClient }