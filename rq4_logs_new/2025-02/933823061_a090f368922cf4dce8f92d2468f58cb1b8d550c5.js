const express = require("express");
const cors = require("cors");
const bodyParser = require("express").json;

const app = express();

app.use(cors());
app.use(bodyParser());

module.exports = app;