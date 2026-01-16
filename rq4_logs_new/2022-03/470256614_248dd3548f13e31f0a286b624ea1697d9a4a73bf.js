const calculate = {
  amountNubers: document.querySelector("input#amount-numbers"),
  minNumber: document.querySelector("input#min-number"),
  maxNumber: document.querySelector("input#max-number"),
  numberResult: document.getElementById("number-result"),
  numbers: document.getElementById("numbers"),

  getValues() {
    return {
      minNumber: calculate.minNumber.value,
      maxNumber: calculate.maxNumber.value,
      amountNumbers: calculate.amountNubers.value,
    };
  },

  draw() {
    let { minNumber, maxNumber } = calculate.getValues();

    minNumber = Number(minNumber);
    maxNumber = Number(maxNumber);

    resultNumber = Math.random() * (maxNumber - minNumber) + minNumber;

    calculate.numberResult.textContent = resultNumber.toFixed(0);
  },
};