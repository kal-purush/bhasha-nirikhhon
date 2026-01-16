const express = require("express");
const hpiGenerator = require("hpi-generator");

var app = express.Router();

app.post("/", function (req, res) {
  // receives values extracted from patient's responses by Watson assistant 
  // and uses the hpi-generator module to create and return an HPI
  const data = req.body;
  const hpi = hpiGenerator.createHPI(data);

  // TODO: Persist the response

  return res.json({
    response: {
      text: hpi
    }
  });
});

module.exports = app;