import { SignOptions } from "jsonwebtoken";

export interface IUserAuthToken extends SignOptions {
  _id: string;
  username: string;
  lastUpdatedAt: Date;
  iat: number;
}