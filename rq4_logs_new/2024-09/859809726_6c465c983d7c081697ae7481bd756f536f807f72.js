document
  .getElementById("cash-out-button")
  .addEventListener("click", function (event) {
    event.preventDefault();
    const cashOutAmount = document.getElementById("cash-out-amount").value;
    const cashOutAmountNumber = Number(cashOutAmount);

    const currentBalance = document.getElementById("balance").innerText;
    const currentBalanceNumber = Number(currentBalance);

    const pin = document.getElementById("cash-out-pin").value;

    if (pin !== "1234") {
      alert("Something went wrong! please try again later");
    }

    const newBalance = currentBalanceNumber - cashOutAmountNumber;
    let fixed = newBalance.toFixed(2)
    console.log(fixed);
    
    document.getElementById("balance").innerText = fixed;
  });