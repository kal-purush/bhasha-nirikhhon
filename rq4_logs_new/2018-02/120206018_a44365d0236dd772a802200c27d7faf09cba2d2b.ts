import * as path from "path";
import * as webpack from "webpack";
import * as webpackMiddleware from "webpack-dev-middleware";
import * as webpackHotMiddleware from "webpack-hot-middleware";
import * as express from "express";
import {Application} from "express";
import {Compiler} from "webpack";

import devConfig from "./webpack.config";

let config;
if (process.env.NODE_ENV === 'development') {
  config = devConfig;
} else {
  config = {};
}

const app: Application = express();
const compiler: Compiler = webpack(config);

app.use(webpackMiddleware(compiler, {
  publicPath: config.output.publicPath,
}));

if (process.env.NODE_ENV === 'development') {
  app.use(webpackHotMiddleware(compiler));
}

app.use(express.static(path.join(__dirname, "../public")));

app.listen(3000, () => {
  console.log("Server is listening on port 3000!");
});