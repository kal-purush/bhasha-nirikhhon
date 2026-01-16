const express = require("express"); 
const bodyParser = require("body-parser");

const app = express();
app.use(bodyParser.urlencoded({extended: true}));

app.get("/", function(req, res) {
	res.sendFile(__dirname + "/index.html");
})

app.post("/", function(req, res) {

	var num1 = Number(req.body.n1);
	var num2 = Number(req.body.n2);

	var result = num1+num2;
	res.send("The result of the calculation is "+result);
})

app.get("/bmicalculator", function(req, res) {
	res.sendFile(__dirname + "/bmiCalculator.html");
})

app.post("/bmicalculator", function(req, res) {

	var num1 = parseFloat(req.body.weight);
	var num2 = parseFloat(req.body.height);

	var result = num1/(num2*num2);
	res.send("Your BMI is "+result);
})


app.listen(3000, function() {
	console.log("Sever started on port 3000");
});

