"use strict";

const express = require("express");
const bodyParser = require("body-parser");

const restService = express();

restService.use(
  bodyParser.urlencoded({
    extended: true
  })
);

restService.use(bodyParser.json());

restService.post("/div", function(req, res) {
  var divnum =
    req.body.result &&
    req.body.result.parameters &&
    req.body.result.parameters.number/10
      ? req.body.result.parameters.echoText
      : "Seems like some problem. Speak again.";
  return res.json({
    divnum: divnum,
    displayText: divnum,
    source: "willmaths"
  });
});