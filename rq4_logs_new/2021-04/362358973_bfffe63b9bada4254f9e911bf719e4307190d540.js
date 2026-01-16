function calculateTip() {
    let amount = document.getElementById("amount").value;
    let levelService = document.getElementById("radio-button").value;
    let persons = document.getElementById("persons").value;

    let total = (amount*levelService)/persons;
    total = Math.round(total * 100)/100;
    total = total.toFixed(2);

    document.getElementById("tip").innerHTML = total;
};

document.getElementById("calculate").onclick = function() {
    calculateTip();
};