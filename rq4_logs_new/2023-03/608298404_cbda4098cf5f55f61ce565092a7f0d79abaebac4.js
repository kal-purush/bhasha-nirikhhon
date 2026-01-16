const express = require("express");
const app = express();
app.listen(3000, function () {
  console.log("Server started on port 3000");
}); //port 3000 (listen to that port for any http request)