const display = document.querySelector(".display");
const buttons = document.querySelectorAll("button");
const specialChars = ["%", "*", "/", "-", "+", "="];
let output = "";
let memory = "";
let mrcClickCount = 0;
let decimalClicked = false;

const formatNumber = (num) => {
  const numString = num.toString();
  if (numString.length > 12) {
    return num.toExponential(8);
  }
  return numString;
};

const calculate = (btnValue) => {
  display.focus();

  if (output === "" && specialChars.includes(btnValue)) {
    return;
  }

  const lastCharIsOperator = specialChars.includes(
    output.charAt(output.length - 1)
  );

  if (btnValue === "=" && output !== "") {
    output = formatNumber(eval(output.replace("%", "/100")));
  } else if (btnValue === "AC") {
    output = "";
    decimalClicked = false;
  } else if (btnValue === "DEL") {
    output = formatNumber(output.toString().slice(0, -1));
  } else if (btnValue === "BIN") {
    output = formatNumber((output >>> 0).toString(2));
  } else if (specialChars.includes(btnValue)) {
    if (!lastCharIsOperator) {
      if (btnValue !== ".") {
        decimalClicked = false;
      }
      output += btnValue;
    }
  } else if (btnValue === "M+") {
    memory = eval(memory + parseFloat(eval(output)));
    decimalClicked = false;
  } else if (btnValue === "M-") {
    memory = eval(memory - parseFloat(eval(output)));
    decimalClicked = false;
  } else if (btnValue === "MC") {
    memory = "";
  } else if (btnValue === "MR") {
    output += memory;
  } else if (btnValue === ".") {
    if (!decimalClicked) {
      output += btnValue;
      decimalClicked = true;
    }
  } else {
    output += btnValue;
  }

  display.value = output;
  console.log(memory + typeof memory);
};

buttons.forEach((button) => {
  button.addEventListener("click", (e) => calculate(e.target.dataset.value));
});