import "reflect-metadata";
import Kinesis from "lifion-kinesis";
import * as AWS from "aws-sdk";
AWS.config.update({ region: "eu-west-1" });

let numberOfSentData = 0;
const kinesis = new Kinesis({
  streamName: "elvenite-demo-stack-kinesisstream7162E27C-1SXQJJ1MDZDI2"
});

kinesis.on("data", data => {
  console.log(
    "Incoming data:",
    data.records.map(v => v.data),
    numberOfSentData++
  );
});

kinesis.startConsumer();
// const bootstrap = async () => {
//   const express = require("express");
//   const app = express();
//   const port = 3000;

//   app.get("/", (req, res) => res.send("Hello World!"));

//   app.listen(port, () => console.log(`Example app listening on port ${port}!`));
// };

// bootstrap();