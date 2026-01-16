const express = require("express");

const app = express();
const HTTP_PORT = process.env.HTTP_PORT || 3000;

app.use(express.static(__dirname + "/build"));
app.listen(HTTP_PORT);

console.log(`Listening to port ${HTTP_PORT}...`);