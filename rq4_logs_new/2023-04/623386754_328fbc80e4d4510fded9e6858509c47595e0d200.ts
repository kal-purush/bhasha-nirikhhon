// NPMS
import dotenv from "dotenv";
dotenv.config();

import "express-async-errors";

// EXPRESS
import express from "express";
const app = express();

// DATABASE
import { connectDB } from "./connectDB/connectDB";

// ENV VARIABLES
const mongo_url = process.env.MONGO_URL as string;
const port = Number(process.env.PORT) || 5000;

// ROUTES
import authRouter from "./routes/authRoutes";

// MIDDLEWARES
app.use(express.json());

// PAGES
app.use("/api/v1/auth", authRouter);

// START SERVER
const start = async () => {
  try {
    await connectDB(mongo_url);
    app.listen(port, () =>
      console.log(`Server is listening on port: ${port}...`)
    );
  } catch (error) {
    console.log(error);
    process.exit(0);
  }
};

start();