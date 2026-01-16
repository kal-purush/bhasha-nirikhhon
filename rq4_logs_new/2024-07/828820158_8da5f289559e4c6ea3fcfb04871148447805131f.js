import express from "express";
import mongoose from "mongoose";
import router from "./routes/user-route.js";
import jobRouter from "./routes/job-route.js";
import compRouter from "./routes/company-route.js";
import dotenv from "dotenv";

dotenv.config();

const app = express();

app.use(express.json());
app.use("/api/user", router);
app.use("/api", jobRouter);
app.use("/api", router);

app.use("/api", compRouter);

const MONGODB_URI = process.env.MONGODB_URI;

mongoose.connect(MONGODB_URI
	)
.then(() => app.listen(5000))
.then(() =>console.log("Connected to Database and listening on port 5000"))
.catch ((err) => console.log(err));