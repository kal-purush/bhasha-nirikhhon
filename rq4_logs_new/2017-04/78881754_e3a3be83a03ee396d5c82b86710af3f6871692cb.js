/*jslint nomen: true */
/*globals require, console, __dirname */
var express = require("express");
var app = express();

app.get("/", function (req, res) {
    console.log("Request: [" + req.headers.host + "]: /index.html");
    res.sendFile(__dirname + "/index.html");
});

/* servers all the static files */
app.get(/^(.+)$/, function (req, res) {
    console.log("Request: [" + req.headers.host + "]: " + req.params[0]);
    res.sendFile(__dirname + req.params[0]);
});

console.log("HTTP server is ready.");
app.listen(8080);