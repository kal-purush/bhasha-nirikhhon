import * as dotenv from "dotenv";
import * as path from "path";
dotenv.config({
  path: path.join(__dirname, "..", "config", "environments", "process.env")
});
import * as config from "config";
import * as debug from "debug";
import * as express from "express";
import * as helmet from "helmet";
import * as morgan from "morgan";

const port = config.get("port");
const logger = debug("app:startup");
const app = express();

if (config.get("morgan.enabled")) {
  app.use(morgan(config.get("morgan.format")));
  logger(`Morgan started as '${config.get("morgan.format")}'...`);
}

if (config.get("helmet.enabled")) {
  app.use(helmet());
  logger(`Helmet started...`);
}

app.use(express.static(path.join(__dirname, "..", "public")));
logger(`Public folder in ${path.join(__dirname, "..", "public")}...`);

app.listen(port, () => logger(`Listening on port ${port}...`));