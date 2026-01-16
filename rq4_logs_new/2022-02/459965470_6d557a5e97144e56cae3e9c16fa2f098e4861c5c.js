function getPin() {
  const pin = Math.round(Math.random() * 10000);
  const pinString = pin + "";
  if (pinString.length == 4) {
    return pin;
  } else {
    return getPin();
  }
}

document.getElementById("generate-pin").addEventListener("click", function () {
  const pin = getPin();
  document.getElementById("generate-input").value = pin;
});

document.getElementById("key-pad").addEventListener("click", function (event) {
  const number = event.target.innerText;
  const inputNumber = document.getElementById("cal-input");
  if (isNaN(number)) {
    if (number == "C") {
      inputNumber.value = "";
    }
  } else {
    const currentNumber = inputNumber.value;
    const total = currentNumber + number;
    inputNumber.value = total;
  }
});

function matchCode() {
  const pins = document.getElementById("generate-input").value;
  const inputNumbers = document.getElementById("cal-input").value;

  const verifySuccess = document.getElementById("verify-success");
  const verifyFail = document.getElementById("verify-fail");
  if (pins == inputNumbers) {
    verifySuccess.style.display = "block";
    verifyFail.style.display = "none";
  } else {
    verifyFail.style.display = "block";
    verifySuccess.style.display = "none";
  }
}

document.getElementById("verify-btn").addEventListener("click", function () {
  matchCode();
});