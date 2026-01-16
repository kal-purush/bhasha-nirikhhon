"use strict";
const express = require("express");
const router_1 = require('./router');
let app = express();
app.use(router_1.router);
app.listen(3000);