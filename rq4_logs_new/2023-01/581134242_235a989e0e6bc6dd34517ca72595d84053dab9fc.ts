import { config } from "dotenv";

config();


export const Port = process.env['PORT'] as string;
export const MongoUri = process.env['MONGODB_URI'] as string;
export const secretAuth = process.env['SECRET'] as string;