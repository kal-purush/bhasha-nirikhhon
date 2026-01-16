document.addEventListener("DOMContentLoaded", function () {
  const billInput = document.getElementById("billAmount");
  const numPeopleInput = document.getElementById("numberOfPeople");
  const tipRadios = document.getElementsByName("tipPercentage");
  const customTipInput = document.getElementById("customTipValue");

  function calculateTip() {
    let billAmount = parseFloat(billInput.value);
    let numberOfPeople = parseInt(numPeopleInput.value);
    let tipPercentage = 0;

    for (let i = 0; i < tipRadios.length; i++) {
      if (tipRadios[i].checked) {
        tipPercentage = parseFloat(tipRadios[i].value);
        if (tipPercentage === 0) {
          customTipInput.disabled = false;
          tipPercentage = parseFloat(customTipInput.value);
        } else {
          customTipInput.disabled = true;
        }
        break;
      }
    }

    if (
      !isNaN(billAmount) &&
      billAmount >= 0 &&
      !isNaN(numberOfPeople) &&
      numberOfPeople >= 1
    ) {
      let tipAmount = (billAmount * tipPercentage) / 100;
      let totalBill = billAmount + tipAmount;
      let billPerPerson = totalBill / numberOfPeople;
      let tipPerPerson = tipAmount / numberOfPeople;

      document.getElementById("totalBill").textContent =
        "$" + totalBill.toFixed(2);
      document.getElementById("billPerPerson").textContent =
        "$" + billPerPerson.toFixed(2);
      document.getElementById("tipPerPerson").textContent =
        "$" + tipPerPerson.toFixed(2);
    }
  }

  billInput.addEventListener("input", calculateTip);
  numPeopleInput.addEventListener("input", calculateTip);

  tipRadios.forEach((radio) => {
    radio.addEventListener("change", calculateTip);
  });

  customTipInput.addEventListener("input", calculateTip);

  function resetCalculator() {
    billInput.value = "";
    numPeopleInput.value = "";
    customTipInput.value = "";
    customTipInput.disabled = true;

    tipRadios.forEach((radio) => {
      if (radio.value === "") {
        radio.checked = true;
      } else {
        radio.checked = false;
      }
    });

    document.getElementById("totalBill").textContent = "0.00";
    document.getElementById("billPerPerson").textContent = "$0.00";
    document.getElementById("tipPerPerson").textContent = "$0.00";
  }

  const resetButton = document.querySelector("button");
  resetButton.addEventListener("click", resetCalculator);
});