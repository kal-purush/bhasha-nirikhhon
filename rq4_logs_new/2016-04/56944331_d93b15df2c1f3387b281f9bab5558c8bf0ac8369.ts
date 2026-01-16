"use strict";

import * as bodyParser from "body-parser";
import * as express from "express";
import * as path from "path";

var app = express();

app.get('/', (req: express.Request, res: express.Response) => {
  res.send('Hello Typescript!');
});

app.get('/:name', (req: express.Request, res: express.Response) => {
  let name: string = req.params.name;
  res.send('Hello ' + name +'!');
});

app.listen('8080');