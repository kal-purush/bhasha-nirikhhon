const TAXENGINE = require("./modules/TAXENGINE");

const express = require("express");
const cors = require("cors");
var app = express();

app.use(cors());
app.use(express.json());

// End-point called to calculate tax
app.post("/calcTax", function (req, res) {
  TAXENGINE.CalcTax(req.body, function (error, resp) {
    if (error) {
      console.error("Error - " + error);
      res.send(error);
    } else {
      res.status(200).send(resp);
    }
  });
});

var port = process.env.PORT || 30000;
app.listen(port, function () {
  console.log("TAX ENGINE app listening on port " + port);
});