const express = require("express"),
  cors = require("cors"),
  bodyParser = require("body-parser"),
  port = 3001,
  app = express();

app.use(cors());
app.use(bodyParser.json());

let count = 0;

app.get("/api/test", (req, res) => {
  debugger;
  count++;
  return res.status(200).json(count);
});

app.listen(port, () => {
  console.log("Server listening on port", port);
});