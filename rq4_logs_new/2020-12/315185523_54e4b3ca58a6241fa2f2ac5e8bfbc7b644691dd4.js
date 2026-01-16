var derivative = document.getElementById("der");
var integral = document.getElementById("int");

derivative.addEventListener("click", derivativeClick);
integral.addEventListener("click", integralClick);

function derivativeClick() {
  document.getElementById("deriv").style.display = "table";
  document.getElementById("intg").style.display = "none";
}

function integralClick() {
  document.getElementById("deriv").style.display = "none";
  document.getElementById("intg").style.display = "table";
}