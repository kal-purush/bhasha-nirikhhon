const express = require("express");

const PORT = process.env.PORT || 8080;

const app = express();

app.use(express.static("public"));
app.use(express.static("node_modules/three/build"));
app.use(express.static("node_modules/three/examples/jsm/controls"));
app.use(express.static("node_modules/three/examples/jsm/loaders"));


app.listen(PORT);
console.log("Listening port: " + PORT);