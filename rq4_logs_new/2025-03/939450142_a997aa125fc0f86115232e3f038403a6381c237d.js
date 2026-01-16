import express from "express";
import connectToDB from "./db/connect.db.js";

const app = express();

connectToDB();

export default app;