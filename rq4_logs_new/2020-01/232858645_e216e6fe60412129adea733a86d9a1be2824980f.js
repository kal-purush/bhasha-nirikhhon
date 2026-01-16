//jshint esversion:6

const express = require("express");
const bodyParser = require("body-parser");

var app = express();
app.use(bodyParser.urlencoded({extended:true}));

app.get("/", function(req,res)
{
  res.sendFile(__dirname + "/index.html");
});

app.post("/", function(req, res)
{
  var num1 = Number(req.body.num1);
  var num2 = Number(req.body.num2);
  var results =  num1 + num2;
  res.send("The results is " + results);
});

app.get("/bmicalc", function(req,res)
{
  res.sendFile(__dirname + "/bmicalc.html");
});
app.post("/bmicalc.html", function(req,res){
  var w = parseFloat(req.body.Weight);
  var h = parseFloat(req.body.Height);
  var bmi = w/(w*h);
  res.send("Your BMI is " + bmi);
});
app.listen(3000, function() {
console.log("server is listening on port 3000");
});