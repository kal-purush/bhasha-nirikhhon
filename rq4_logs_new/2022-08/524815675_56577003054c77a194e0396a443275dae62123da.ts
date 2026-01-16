import express from "express";
import config from "config";
import dbConnect from "./utils/dbConnect";
import logger from "./utils/logger";
import cors, { CorsOptions } from "cors";
import routes from "./routes";
import corsOptions from "./utils/corsOptions";

require("dotenv").config();

const PORT = config.get<number>("port");
const app = express();

app.use(cors(corsOptions as CorsOptions));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.listen(PORT, async () => {
  logger.info(`Api is running at http://localhost:${PORT}/api`);
  await dbConnect();
  routes(app);
});