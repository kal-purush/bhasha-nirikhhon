const express = require("express");
const app = express();

app.get("/", function (res, req) {
  res.send("Hello, My Name is Phil");
});

app.listen(process.env.PORT || 5000);