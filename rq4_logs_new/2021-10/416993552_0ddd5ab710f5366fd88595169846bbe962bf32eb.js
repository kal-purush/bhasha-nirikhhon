//jshint esversion:6

// NPM Section
const express = require("express");
const bodyParser = require("body-parser");

const app = express();
const port = 3000;

var errorMessage = "<h1>404</h1><p>Sorry it seems there was an error in the input you gave, please try again.</p>";

app.use(bodyParser.urlencoded({extended: true}));

// Home Section
app.get("/", function(req, res){
  res.sendFile(__dirname + "/index.html");
});

app.post("/", function(req, res){

  var num1 = Number(req.body.n1);
  var num2 = Number(req.body.n2);
  var operator = req.body.op;

  function calculate(n1, n2, op) {
    var result = 0;

    if (operator == "+") {
      result = n1+n2;
      res.send("The sum of " + n1 + " and " + n2 + " is " + result);
    } else if (operator == "-") {
      result = n1-n2;
      res.send("The difference of " + n1 + " and " + n2 + " is " + result);
    } else if (operator == "/") {
      result = n1/n2;
      res.send("The quotient of " + n1 + " and " + n2 + " is " + result);
    } else if (operator == "*") {
      result = n1*n2;
      res.send("The product of " + n1 + " and " + n2 + " is " + result);
    } else {
      res.send(errorMessage);
    }
  }

  calculate(num1, num2, operator);

});

// BMI Calculator Section
app.get("/bmicalculator", function(req, res){
  res.sendFile(__dirname + "/bmiCalculator.html");
});

app.post("/bmicalculator", function(req,res){

  var weight = parseFloat(req.body.w);
  var height = parseFloat(req.body.h);

  var result = w/Math.pow(h, 2);
  res.send("Your BMI is " + result);

});

app.listen(port, function(){
  console.log("Server started properly on port " + port + ".");
});