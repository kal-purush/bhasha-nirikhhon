//Value id variable
const inputValue = document.getElementById("input-value");

const total = document.getElementById("total");
const value1 = document.getElementById("value1");
const value2 = document.getElementById("value2");
const value3 = document.getElementById("value3");
const value4 = document.getElementById("value4");
const value5 = document.getElementById("value5");
const value6 = document.getElementById("value6");
const value7 = document.getElementById("value7");
const value8 = document.getElementById("value8");
const value9 = document.getElementById("value9");
const value0 = document.getElementById("value0");
const valueMultiple = document.getElementById("value-multiple");
const valueModulo = document.getElementById("modulo");
const valueSum = document.getElementById("value-sum");
const valueDivison = document.getElementById("value-divison");
const valueSubstrat = document.getElementById("value-substrat");
const valueDot = document.getElementById("value-dot");
const valueEqual = document.getElementById("equal");
const valueDel = document.getElementById("delete");

function input (para) {
    const inputValueText = inputValue.innerText;
    inputValue.innerText = inputValueText + para.innerText;
}
// Functionality
document.getElementById("all-clear").addEventListener("click", function () {
  inputValue.innerText = "";
  total.innerText = "";
});
value1.addEventListener("click", function () {
    input(value1);
});
value2.addEventListener("click", function () {
  input(value2);
});
value3.addEventListener("click", function () {
  input(value3);
});
value4.addEventListener("click", function () {
  input(value4);
});
value5.addEventListener("click", function () {
  input(value5);
});
value6.addEventListener("click", function () {
  input(value6);
});
value7.addEventListener("click", function () {
  input(value7);
});
value8.addEventListener("click", function () {
  input(value8);
});
value9.addEventListener("click", function () {
  input(value9);
});
value0.addEventListener("click", function () {
  input(value0);
});
valueDot.addEventListener("click", function () {
  input(valueDot);
});
valueDivison.addEventListener("click", function () {
  input(valueDivison);
});
valueMultiple.addEventListener("click", function () {
  input(valueMultiple);
});
valueSubstrat.addEventListener("click", function () {
  input(valueSubstrat);
});
valueSum.addEventListener("click", function () {
  input(valueSum);
});
valueModulo.addEventListener("click", function () {
  input(valueModulo);
});
valueDel.addEventListener("click", function () {
  const inputValueText = inputValue.innerText;
var lastCharOfHello=inputValueText.slice(0, -1);
inputValue.innerText = lastCharOfHello;
});
valueEqual.addEventListener("click", function () {
  total.innerText = eval(inputValue.innerText);
});