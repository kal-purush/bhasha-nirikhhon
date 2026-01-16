import mysql2 from "mysql2/promise";
import * as Session from "express-session";
import expressMySqlSession from "express-mysql2-session";
import { config } from "../config.js";

declare module "express-session" {
  export interface SessionData {
    is_logined?: boolean;
    dispayName?: string;
    userId?: number;
  }
}

const connection = mysql2.createPool(config.db);
const MySQLStore = expressMySqlSession(Session);
const sessionStore = new MySQLStore({}, connection);
export const sessionOption = {
  secret: config.session.secreatKey,
  resave: false,
  saveUninitialized: false,
  store: sessionStore,
  cookie: {
    httpOnly: true,
    secure: false,
    maxAge: 3600000,
  },
};