
function Add() {
  var x = parseFloat(document.getElementById("num1").value);
  var y = parseFloat(document.getElementById("num2").value);
}

function Subtract() {
  var x = parseFloat(document.getElementById("num1").value);
  var y = parseFloat(document.getElementById("num2").value);
}

function Multiply() {
  var x = parseFloat(document.getElementById("num1").value);
  var y = parseFloat(document.getElementById("num2").value);
}

function Divide() {
  var x = parseFloat(document.getElementById("num1").value);
  var y = parseFloat(document.getElementById("num2").value);
}

function Remainder() {
  var x = parseFloat(document.getElementById("num1").value);
  var y = parseFloat(document.getElementById("num2").value);
}

function Print() {
  document.getElementById("demo").innerHTML = "The remainder of " + x + " and " + y + " is " + (x % y) + ".";
}