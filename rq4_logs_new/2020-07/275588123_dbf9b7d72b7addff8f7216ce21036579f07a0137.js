//console.log("Hello this is a console message!");

function calcAmount(){
    let amountInput = document.querySelector("input[name='amount-input']");
    let amount = parseInt(amountInput.value) * price;
    alert("Payable: £ " + amount);
}