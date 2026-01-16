const express = require("express");
const bodyParser = require("body-parser")
const fs = require('fs')

const app = express();

const pathToFile = "/home/pi/pool/tempIN"

app.use(bodyParser.json());

app.post("/", (req, res) => {
  fs.writeFileSync(pathToFile, req.body.temperature);
  res.send("ok").status(200);
})


app.listen(4545, () => {
  console.log("Server is up");
})