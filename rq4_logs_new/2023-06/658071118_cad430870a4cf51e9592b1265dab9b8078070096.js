var express = require("express");
var app = express();
var mongoose = require("mongoose");
var apiController = require("./controllers/apiController");

var port = 3000;

app.use("/assets", express.static(__dirname + "/public"));
//mongo instead of localhost for docker-compose

main().catch((err) => console.log(err));
async function main() {
  await mongoose
    .connect(
      `mongodb://${
        process.env.MONGO_HOSTNAME || "localhost"
      }:27017/node-api-demo`,
      {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      }
    )
    .then(() => console.log("Connected."));
}

apiController(app);

app.listen(port);