const userInput = document.getElementById("userInput");
let expression = "";

const press = function (value) {
  expression += value;
  userInput.value = expression;
};

const equal = function () {
  userInput.value = eval(expression);
  expression = "";
};

const erase = function () {
  expression = "";
  userInput.value = expression;
};