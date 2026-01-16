//jshint esversion:6

const express = require("express");
const bodyParser = require("body-parser");

const app = express();
app.use(express.static("public"));

app.get("/", function(req, res){
  res.sendFile(__dirname + "/index.html");
});

app.get("/contact", function(req, res){
  res.sendFile(__dirname + "/contact.html");
});

app.get("/about", function(req, res){
  res.sendFile(__dirname + "/about.html");
});

app.get("/gallery", function(req, res){
  res.sendFile(__dirname + "/gallery.html");
});

app.get("/faq", function(req, res){
  res.sendFile(__dirname + "/faq.html");
});

app.get("/attorney", function(req, res){
  res.sendFile(__dirname + "/attorney.html");
});

app.listen(4000, function(){
  console.log("Server is running on port 4000");
});